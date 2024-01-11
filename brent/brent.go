package brent

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"github.com/acorn-io/brent/pkg/attributes"
	"github.com/acorn-io/brent/pkg/auth"
	"github.com/acorn-io/brent/pkg/schema"
	"github.com/acorn-io/brent/pkg/schema/converter"
	brent "github.com/acorn-io/brent/pkg/server"
	"github.com/acorn-io/brent/pkg/server/router"
	"github.com/acorn-io/brent/pkg/types"
	"github.com/acorn-io/mink/brent/reqhost"
	"github.com/acorn-io/mink/pkg/authz"
	mserver "github.com/acorn-io/mink/pkg/server"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
)

type RoundTripFunc func(*http.Request) (*http.Response, error)

func (r RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return r(req)
}

type Config struct {
	RESTConfig *rest.Config
	MinkConfig *mserver.Config
	Authz      authz.BindingAuthorizer
}

func Handler(ctx context.Context, cfg *Config) (http.Handler, genericapiserver.PostStartHookFunc, error) {
	var (
		next http.Handler
		lock sync.RWMutex
	)

	handler := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		lock.RLock()
		h := next
		lock.RUnlock()
		if h == nil {
			rw.WriteHeader(http.StatusServiceUnavailable)
		} else {
			h.ServeHTTP(rw, req)
		}
	})

	start := genericapiserver.PostStartHookFunc(func(context genericapiserver.PostStartHookContext) error {
		h, err := start(ctx, context, cfg)
		if err != nil {
			return err
		}
		lock.Lock()
		next = h
		lock.Unlock()
		return nil
	})

	return handler, start, nil
}

func start(ctx context.Context, hookContext genericapiserver.PostStartHookContext, cfg *Config) (http.Handler, error) {
	controllers, err := brent.NewController(cfg.RESTConfig)
	if err != nil {
		return nil, err
	}

	restConfig := rest.CopyConfig(cfg.RESTConfig)
	restConfig.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return RoundTripFunc(func(req *http.Request) (*http.Response, error) {
			host := reqhost.FromContext(req.Context())
			if host != "" {
				req.Header.Add("X-Forwarded-Host", host)
			}
			return rt.RoundTrip(req)
		})
	})

	var (
		k8sHandler  http.Handler
		bindingAuth = cfg.Authz
	)
	if bindingAuth == nil {
		bindingAuth = authz.NewAllowAll()
	}

	s, err := brent.New(ctx, restConfig, &brent.Options{
		AuthMiddleware: toAuthMiddleware(cfg.MinkConfig.Authenticator),
		AccessSetLookup: &AccessSetLookup{
			authorizer: bindingAuth,
		},
		Router: func(h router.Handlers) http.Handler {
			k8sHandler = h.K8sProxy
			return router.Routes(h)
		},
		Controllers: controllers,
	})
	if err != nil {
		return nil, err
	}

	subResources, err := newSubResources(s.BaseSchemas, k8sHandler, cfg.MinkConfig.APIGroups)
	if err != nil {
		return nil, err
	}

	s.SchemaFactory.AddTemplate(schema.Template{
		Formatter: func(request *types.APIRequest, resource *types.RawResource) {
			if m, err := meta.Accessor(resource.APIObject.Object); err == nil {
				m.SetManagedFields(nil)
			}
		},
		Customize: func(apiSchema *types.APISchema) {
			gvr := attributes.GVR(apiSchema)
			if gvr.Resource == "" {
				attributes.SetVerbs(apiSchema, nil)
				return
			}
			if !strings.HasSuffix(gvr.Group, ".k8s.io") {
				subResources.Customize(apiSchema)
			} else {
				//attributes.SetVerbs(apiSchema, nil)
			}
		},
	})

	schemas := map[string]*types.APISchema{}
	client, err := discovery.NewDiscoveryClientForConfig(hookContext.LoopbackClientConfig)
	if err != nil {
		return nil, err
	}
	if err := converter.AddOpenAPI(client, schemas); err != nil {
		return nil, err
	}
	if err := converter.AddDiscovery(client, schemas); err != nil {
		return nil, err
	}
	s.SchemaFactory.(*schema.Collection).Reset(filter(schemas))
	return s, nil
}

func filter(schemas map[string]*types.APISchema) map[string]*types.APISchema {
	filteredSchemas := map[string]*types.APISchema{}
	for _, schema := range schemas {
		if isListWatchable(schema) {
			if preferredTypeExists(schema, schemas) {
				continue
			}
		}

		gvk := attributes.GVK(schema)
		if gvk.Kind != "" {
			gvr := attributes.GVR(schema)
			schema.ID = converter.GVKToSchemaID(gvk)
			schema.PluralName = converter.GVRToPluralName(gvr)
		}
		filteredSchemas[schema.ID] = schema
	}
	return filteredSchemas
}

func toAuthMiddleware(authn authenticator.Request) auth.Middleware {
	if authn == nil {
		return nil
	}
	return auth.ToMiddleware(auth.AuthenticatorFunc(func(req *http.Request) (user.Info, bool, error) {
		resp, ok, err := authn.AuthenticateRequest(req)
		if resp == nil {
			return nil, ok, err
		}
		return resp.User, ok, err
	}))
}

func isListWatchable(schema *types.APISchema) bool {
	var (
		canList  bool
		canWatch bool
	)

	for _, verb := range attributes.Verbs(schema) {
		switch verb {
		case "list":
			canList = true
		case "watch":
			canWatch = true
		}
	}

	return canList && canWatch
}

func preferredTypeExists(schema *types.APISchema, schemas map[string]*types.APISchema) bool {
	pg := attributes.PreferredGroup(schema)
	pv := attributes.PreferredVersion(schema)
	if pg == "" && pv == "" {
		return false
	}

	gvk := attributes.GVK(schema)
	if pg != "" {
		gvk.Group = pg
	}
	if pv != "" {
		gvk.Version = pv
	}

	_, ok := schemas[converter.GVKToVersionedSchemaID(gvk)]
	return ok
}
