package reqhost

import (
	"context"
	"net/http"
)

type reqHostKey struct{}

func FromContext(ctx context.Context) string {
	v, _ := ctx.Value(reqHostKey{}).(string)
	return v
}

func WithRequestHost(req *http.Request) *http.Request {
	host := req.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = req.Host
	}
	return req.WithContext(context.WithValue(req.Context(), reqHostKey{}, host))
}
