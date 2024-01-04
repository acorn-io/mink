package aggregator

import (
	"fmt"
	minkopenapi "github.com/acorn-io/mink/pkg/openapi"
	"github.com/acorn-io/mink/pkg/proxy"
	minkserver "github.com/acorn-io/mink/pkg/server"
	"github.com/sirupsen/logrus"
	"k8s.io/api/apidiscovery/v2beta1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/filters"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"net/http"
	"net/url"
	"sync"
)

// Delegate represents a "downstream" api server.
type Delegate struct {
	Config *rest.Config
}

// URL returns a url.URL from the Delegate's rest config
func (d *Delegate) URL() *url.URL {
	ret, err := url.Parse(d.Config.Host)
	if err == nil {
		return ret
	}

	return &url.URL{
		Host:   d.Config.Host,
		Path:   d.Config.APIPath,
		Scheme: "https",
	}
}

// Config holds mink server config as well as the list of delegates which this aggregator
// shall "consume".
type Config struct {
	minkserver.Config
	Delegates []*Delegate
}

// NewAggregator creates a new aggregator (returned mink server w/ agggregation functionality) for the given config
func NewAggregator(config *Config) (*minkserver.Server, error) {
	serverConfig, err := minkserver.Prep(&config.Config)
	if err != nil {
		return nil, err
	}

	// we are going to register our own handlers for /openapi/v{2,3}
	serverConfig.SkipOpenAPIInstallation = true

	serverConfig.LongRunningFunc = filters.BasicLongRunningRequestCheck(
		sets.NewString(config.LongRunningVerbs...),
		sets.NewString(config.LongRunningResources...),
	)

	// likely already enabled but just to be safe
	serverConfig.EnableDiscovery = true

	svr, err := serverConfig.Complete().New(config.Name, server.NewEmptyDelegateWithCustomHandler(http.NotFoundHandler()))
	if err != nil {
		return nil, err
	}

	// we need a spec proxier in order to serve v2, or proxy v3 if the delegate supports it
	specProxier := minkopenapi.New()

	wg := sync.WaitGroup{}

	for _, delegate := range config.Delegates {
		delegate := delegate // var capture

		go func() {
			wg.Add(1)
			defer wg.Done()

			// build a proxy handler for this delegate
			transportConfig, err := delegate.Config.TransportConfig()
			if err != nil {
				logrus.Errorf("error converting restconfig to transportconfig: %s", err.Error())
				return
			}
			proxyHandler, err := proxy.New(delegate.URL(), transportConfig)
			if err != nil {
				logrus.Errorf("error building proxy handler for endpoint %s: %s", delegate.URL().String(), err.Error())
				return
			}

			// get a list of api groups and resources for the delegate
			// this is how we register group- and resource-specific handlers
			apiGroupList, apiResources, err := getAPIGroups(delegate)
			if err != nil {
				logrus.Error(err)
				return
			}

			// in order to serve up the discovery endpoint at `/apis` we need to
			// register the api group with GenericAPIServer.DiscoveryGroupManager
			for _, apiGroup := range apiGroupList {
				logrus.Infof("registering apigroup %s into DiscoveryGroupManager\n", apiGroup.Name)
				svr.DiscoveryGroupManager.AddGroup(*apiGroup)
			}

			for _, resources := range apiResources {
				// legacy resources (e.g. nodes, pods, configmaps, et al) need to be served by `/api/v1` handlers
				// everything else is group named and served under `/apis`
				if resources.GroupVersion == "" || resources.GroupVersion == "v1" {
					// register legacy handler
					svr.Handler.NonGoRestfulMux.Handle("/api/v1", proxyHandler)
					svr.Handler.NonGoRestfulMux.HandlePrefix("/api/v1/", proxyHandler)
				} else {
					svr.Handler.NonGoRestfulMux.Handle("/apis/"+resources.GroupVersion, proxyHandler)
					svr.Handler.NonGoRestfulMux.HandlePrefix("/apis/"+resources.GroupVersion+"/", proxyHandler)
				}

				gv, err := schema.ParseGroupVersion(resources.GroupVersion)
				if err != nil {
					logrus.Error(err)
					return
				}

				apiResourceDiscovery, err := endpoints.ConvertGroupVersionIntoToDiscovery(resources.APIResources)
				if err != nil {
					logrus.Error(err)
					return
				}

				// this registration powers the ability of the aggregator to correctly return v2beta1.APIVersionDiscovery
				// lists to clients. this is necessary mostly to power calls such as `kubectl api-resources` which requests
				// `/apis` but returning specifically `g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList`
				svr.AggregatedDiscoveryGroupManager.AddGroupVersion(gv.Group, v2beta1.APIVersionDiscovery{
					Version:   gv.Version,
					Resources: apiResourceDiscovery,
					Freshness: v2beta1.DiscoveryFreshnessCurrent,
				})
			}

			// register the delegate with the specProxie
			if err := specProxier.Register(proxyHandler); err != nil {
				logrus.Error(err)
				return
			}
		}()
	}

	wg.Wait()

	// install the spec proxier's handlers into the GenericAPIServer's NonGoRestfulMux
	// this is what causes the GenericAPIServer to serve /openapi/v{2,3}
	// when serverConfig.SkipOpenAPIInstallation = true
	specProxier.InstallV2(svr.Handler.NonGoRestfulMux)
	specProxier.InstallV3(svr.Handler.NonGoRestfulMux)

	return &minkserver.Server{
		MinkConfig:       &config.Config,
		Config:           serverConfig,
		GenericAPIServer: svr,
	}, nil
}

// getAPIGroups builds a discovery client which calls discovery on /api and /apis
// for the delegate. It returns a list of api groups and api resources which we utilize to register
// handlers for the aggregator.
func getAPIGroups(delegate *Delegate) ([]*v1.APIGroup, []*v1.APIResourceList, error) {
	discoveryEndpoint := delegate.URL().String()

	dc, err := discovery.NewDiscoveryClientForConfig(delegate.Config)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating discovery client for endpoint %s: %s", discoveryEndpoint, err.Error())
	}

	return dc.ServerGroupsAndResources()
}
