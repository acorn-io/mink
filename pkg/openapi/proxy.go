package openapi

import (
	"bytes"
	"encoding/json"
	"k8s.io/kube-aggregator/pkg/controllers/openapi/aggregator"
	aggregator3 "k8s.io/kube-aggregator/pkg/controllers/openapiv3/aggregator"
	"k8s.io/kube-openapi/pkg/handler"
	"k8s.io/kube-openapi/pkg/handler3"
	"k8s.io/kube-openapi/pkg/openapiconv"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"net/http"
	"strings"
	"sync"
	"time"
)

// CanHandle defines an interface for resources that can handle http paths via a passed path and handler
type CanHandle interface {
	Handle(path string, handler http.Handler)
}

// CanHandlePrefix defines an interface for resources that can handle
// http paths ending with "/" via a passed path and handler.
// This interface does not (cannot) enforce path ending with a "/", it is up to the implementation to do that.
type CanHandlePrefix interface {
	HandlePrefix(path string, handler http.Handler)
}

// CanHandleWithPrefix defines an interface for resources that can both Handle() and HandlePrefix()
type CanHandleWithPrefix interface {
	CanHandle
	CanHandlePrefix
}

// Endpoint stores the handler and DiscoveryGroupVersion for an OpenAPI v3 endpoint.
// This is used when calling SpecProxier.HandleGroupVersion to serve http via the http.Handler,
// as well as constructing openapi v3 discovery root via SpecProxier.HandleDiscovery
type Endpoint struct {
	Handler http.Handler
	DGV     handler3.OpenAPIV3DiscoveryGroupVersion
}

// SpecProxier either proxies or serves openapi spec. It is the proxy middleware between
// an aggregator and delegate api servers. The proxier does not itself perform proxying
// but rather relies on an http.Handler to do this
type SpecProxier struct {
	mutex     sync.Mutex
	Discovery map[string]Endpoint

	v2Service *handler.OpenAPIService

	v3Spec *spec3.OpenAPI
}

// New creates a new SpecProxier
func New() *SpecProxier {
	return &SpecProxier{
		v2Service: handler.NewOpenAPIService(nil),
		Discovery: map[string]Endpoint{},
	}
}

// Register will attempt to register OpenAPI v2 & v3 spec for the passed handler.
// It is expected that v2 will always be available, but v3 may not be.
// In that case, v2 will be converted to v3 using openapiconv.ConvertV2ToV3
// and then served directly. Otherwise if v3 is available, the discovery root
// will be downloaded and merged into the SpecProxier's discovery map
func (s *SpecProxier) Register(handler http.Handler) error {
	swagger, err := s.downloadV2(handler)
	if err != nil {
		return err
	}

	if err = s.v2Service.UpdateSpec(swagger); err != nil {
		return err
	}

	// try to get v3 discovery root
	v3Root, err := s.downloadV3(handler)
	if err != nil {
		return err
	}

	if v3Root == nil {
		// no v3 at endpoint. convert & serve v2
		s.v3Spec = openapiconv.ConvertV2ToV3(swagger)
	} else {
		s.RegisterRoot(v3Root, handler)
	}

	return nil
}

// downloadV2 downloads the openapi v2 spec from the http.Handler.
func (s *SpecProxier) downloadV2(handler http.Handler) (*spec.Swagger, error) {
	v2Downloader := aggregator.NewDownloader()

	v2Spec, _, _, err := v2Downloader.Download(handler, "")
	if err != nil {
		return nil, err
	}

	return v2Spec, nil
}

// downloadV3 downloads the openapiv3 root from the http.Handler.
func (s *SpecProxier) downloadV3(handler http.Handler) (*handler3.OpenAPIV3Discovery, error) {
	downloader := aggregator3.NewDownloader()

	root, _, err := downloader.OpenAPIV3Root(handler)

	return root, err
}

// RegisterRoot takes a v3 openapi root, and an http.Handler. Iterating over the paths in v3Root,
// the handler is registered as the handler for that path. This is how new v3 roots are merged
// into Discovery and later served via HandleDiscovery
func (s *SpecProxier) RegisterRoot(v3Root *handler3.OpenAPIV3Discovery, handler http.Handler) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// enumerate paths, register into local discovery
	for path, dgv := range v3Root.Paths {
		s.Discovery[path] = Endpoint{
			Handler: handler,
			DGV:     dgv,
		}
	}
}

// HandleDiscovery returns either the openapi v3 root doc (with relative paths, etc.), or if
// the delegate doesn't support v3, it serves the v3 spec (which has been converted from v2 and stored)
func (s *SpecProxier) HandleDiscovery(w http.ResponseWriter, r *http.Request) {
	var out any
	if s.v3Spec != nil {
		out = s.v3Spec
	} else {
		out = s.V3Root()
	}

	serveJSON(out, w, r)
}

// HandleGroupVersion returns either a proxied result from the delegate's openapi v3 handler for a specific path,
// or it serves up the specifically requested path from the saved v3Spec (converted from v2) in the case of the
// delegate not supporting openapi v3.
func (s *SpecProxier) HandleGroupVersion(w http.ResponseWriter, r *http.Request) {
	// /openapi/v3/example.com/blah
	parts := strings.SplitAfterN(r.URL.Path, "/", 4)

	targetGroupVersion := parts[3]

	if s.v3Spec != nil {
		out := s.v3Spec.Paths.Paths[targetGroupVersion]

		serveJSON(out, w, r)
		return
	}

	for k, v := range s.Discovery {
		if targetGroupVersion == k {
			v.Handler.ServeHTTP(w, r)
			return
		}
	}

	w.WriteHeader(404)
}

// V3Root returns the OpenAPIV3Discovery struct stored by the SpecProxier
func (s *SpecProxier) V3Root() handler3.OpenAPIV3Discovery {
	merged := map[string]handler3.OpenAPIV3DiscoveryGroupVersion{}
	for k, p := range s.Discovery {
		merged[k] = p.DGV
	}

	return handler3.OpenAPIV3Discovery{
		Paths: merged,
	}
}

// InstallV2 installs the /openapi/v2 handler into a CanHandle implementation.
// Typically, the CanHandle is going to be a server.GenericAPIServer's Handler.NonGoRestfulMux
func (s *SpecProxier) InstallV2(server CanHandle) {
	s.v2Service.RegisterOpenAPIVersionedService("/openapi/v2", server)

	//server.Handle("/openapi/v2", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	//	serveJSON(s.v2Spec, w, r)
	//}))
}

// InstallV3 installs the /openapi/v3 handler into a CanHandleWithPrefix implementation.
// Typically, the CanHandleWithPrefix is going to be a server.GenericAPIServer's Handler.NonGoRestfulMux
func (s *SpecProxier) InstallV3(server CanHandleWithPrefix) {
	server.Handle("/openapi/v3", http.HandlerFunc(s.HandleDiscovery))
	server.HandlePrefix("/openapi/v3/", http.HandlerFunc(s.HandleGroupVersion))
}

// serveJSON buffers the bytes of, encodes, and then serves the content of obj
func serveJSON(obj any, w http.ResponseWriter, r *http.Request) {
	buf := bytes.NewBuffer([]byte{})

	json.NewEncoder(buf).Encode(obj)

	w.Header().Set("Content-Type", "application/json")

	http.ServeContent(w, r, "", time.Now(), bytes.NewReader(buf.Bytes()))
}
