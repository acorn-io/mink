package stores

import (
	"github.com/acorn-io/mink/pkg/strategy"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Getter             = (*CreateGetListDeleteStore)(nil)
	_ rest.Lister             = (*CreateGetListDeleteStore)(nil)
	_ rest.RESTDeleteStrategy = (*CreateGetListDeleteStore)(nil)
	_ strategy.Base           = (*CreateGetListDeleteStore)(nil)
)

type CreateGetListDelete interface {
	strategy.Getter
	strategy.Creater
	strategy.Lister
	strategy.Deleter
}

type CreateGetListDeleteStore struct {
	*strategy.GetAdapter
	*strategy.CreateAdapter
	*strategy.ListAdapter
	*strategy.DeleteAdapter
	*strategy.DestroyAdapter
}

func (c *CreateGetListDeleteStore) New() runtime.Object {
	return c.CreateAdapter.New()
}

func (r *CreateGetListDeleteStore) NamespaceScoped() bool {
	return r.ListAdapter.NamespaceScoped()
}

func NewCreateGetListDelete(scheme *runtime.Scheme, createGetListDelete CreateGetListDelete) *CreateGetListDeleteStore {
	return &CreateGetListDeleteStore{
		GetAdapter:     strategy.NewGet(createGetListDelete),
		CreateAdapter:  strategy.NewCreate(scheme, createGetListDelete),
		ListAdapter:    strategy.NewList(createGetListDelete),
		DeleteAdapter:  strategy.NewDelete(scheme, createGetListDelete),
		DestroyAdapter: &strategy.DestroyAdapter{},
	}
}
