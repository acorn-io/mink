package proxy

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/proxy"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/client-go/transport"
	"net/http"
	"net/url"
)

// ErrorResponder is an implementation of proxy.ErrorResponder
type ErrorResponder struct {
	w http.ResponseWriter
}

// Handler is a struct that stores a round tripper for a given URL with transport configuration.
// It acts as the proxy for calls made to an aggregator which need to be served by a delegate api server.
type Handler struct {
	transportConfig *transport.Config
	roundTripper    http.RoundTripper
	url             *url.URL
}

// New creates a new Handler
func New(url *url.URL, transportConfig *transport.Config) (*Handler, error) {
	rt, err := transport.New(transportConfig)
	if err != nil {
		return nil, err
	}

	return &Handler{
		transportConfig: transportConfig,
		url:             url,
		roundTripper:    rt,
	}, nil
}

// ServeHTTP handles http requests by proxying the request to Handler's URL via
// the Handler's roundTripper. It uses proxy.NewUpgradeAwareHandler to wrap the transport and serve
// the content from the proxied call.
func (p *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upgrade := httpstream.IsUpgradeRequest(r)

	location := &url.URL{}
	location.Host = p.url.Host
	location.Path = r.URL.Path
	location.RawQuery = r.URL.Query().Encode()
	location.Scheme = p.url.Scheme

	handler := proxy.NewUpgradeAwareHandler(location, p.roundTripper, true, upgrade, ErrorResponder{w: w})

	handler.ServeHTTP(w, r)
}

func (e ErrorResponder) Object(statusCode int, obj runtime.Object) {
	responsewriters.WriteRawJSON(statusCode, obj, e.w)
}

func (e ErrorResponder) Error(_ http.ResponseWriter, _ *http.Request, err error) {
	http.Error(e.w, err.Error(), http.StatusServiceUnavailable)
}
