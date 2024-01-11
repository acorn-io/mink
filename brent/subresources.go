package brent

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/acorn-io/brent/pkg/attributes"
	"github.com/acorn-io/brent/pkg/types"
	"github.com/acorn-io/schemer"
	serverrest "k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
)

type subResources struct {
	defs       map[string]*linkActions
	k8sHandler http.Handler
	schemas    *types.APISchemas
}

type linkActions struct {
	ActionHandlers  map[string]http.Handler
	LinkHandlers    map[string]http.Handler
	ResourceActions map[string]schemas.Action
	Formatter       types.Formatter
}

func newSubResources(schemas *types.APISchemas, k8sHandler http.Handler, apiGroups []*genericapiserver.APIGroupInfo) (*subResources, error) {
	s := &subResources{
		defs:       map[string]*linkActions{},
		k8sHandler: k8sHandler,
		schemas:    schemas,
	}

	stores := map[string]serverrest.Storage{}
	for _, apiGroup := range apiGroups {
		for _, group := range apiGroup.VersionedResourcesStorageMap {
			for k, v := range group {
				stores[k] = v
			}
		}
	}

	return s, s.build(stores)
}

func (s *subResources) build(stores map[string]serverrest.Storage) error {
	for k, v := range stores {
		v := v
		resource, subResource, ok := strings.Cut(k, "/")
		if !ok || subResource == "status" {
			continue
		}

		def, ok := s.defs[resource]
		if !ok {
			def = &linkActions{
				ActionHandlers:  map[string]http.Handler{},
				ResourceActions: map[string]schemas.Action{},
				LinkHandlers:    map[string]http.Handler{},
			}
			s.defs[resource] = def
		}

		input, err := s.schemas.Import(v.New())
		if err != nil {
			return err
		}
		input.ResourceMethods = nil
		input.CollectionMethods = nil

		if _, ok := v.(serverrest.Connecter); ok {
			def.ResourceActions[subResource] = schemas.Action{
				Input: input.ID,
			}
		} else if _, ok := v.(serverrest.Getter); ok {
			if _, ok := v.(serverrest.Creater); ok {
				def.ResourceActions[subResource] = schemas.Action{
					Input:  input.ID,
					Output: input.ID,
				}
			}
		} else {
			def.ResourceActions[subResource] = schemas.Action{
				Input:  input.ID,
				Output: input.ID,
			}
		}

		formatter := func(request *types.APIRequest, resource *types.RawResource) {
			if _, ok := v.(serverrest.Connecter); ok {
				resource.Links[subResource] = s.subResourceURL(subResource, request, resource)
			} else if _, ok := v.(serverrest.Getter); ok {
				resource.Links[subResource] = s.subResourceURL(subResource, request, resource)
				if _, ok := v.(serverrest.Creater); ok {
					resource.Actions[subResource] = s.subResourceURL(subResource, request, resource)
				}
			} else {
				resource.Actions[subResource] = s.subResourceURL(subResource, request, resource)
			}
		}

		if def.Formatter == nil {
			def.Formatter = formatter
		} else {
			def.Formatter = types.FormatterChain(formatter, def.Formatter)
		}
	}

	return nil
}

func (s *subResources) subResourceURL(name string, apiContext *types.APIRequest, resource *types.RawResource) string {
	var nsPath string
	ns, resourceName, ok := strings.Cut(resource.ID, "/")
	if !ok {
		resourceName = ns
	} else {
		nsPath = fmt.Sprintf("namespaces/%s/", ns)
	}
	gvr := attributes.GVR(apiContext.Schema)
	return apiContext.URLBuilder.RelativeToRoot(fmt.Sprintf("/apis/%s/v1/%s%s/%s/%s",
		gvr.Group, nsPath, gvr.Resource, resourceName, name))
}

func (s *subResources) Customize(apiSchema *types.APISchema) {
	gvr := attributes.GVR(apiSchema)
	def, ok := s.defs[gvr.Resource]
	if ok {
		apiSchema.ActionHandlers = def.ActionHandlers
		apiSchema.ResourceActions = def.ResourceActions
		apiSchema.LinkHandlers = def.LinkHandlers
		if def.Formatter != nil {
			if apiSchema.Formatter == nil {
				apiSchema.Formatter = def.Formatter
			} else {
				apiSchema.Formatter = types.FormatterChain(def.Formatter, apiSchema.Formatter)
			}
		}
	}
}
