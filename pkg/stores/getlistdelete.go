package stores

import (
	"github.com/acorn-io/mink/pkg/strategy"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Getter             = (*GetListDeleteStore)(nil)
	_ rest.Lister             = (*GetListDeleteStore)(nil)
	_ rest.RESTDeleteStrategy = (*GetListDeleteStore)(nil)
	_ strategy.Base           = (*GetListDeleteStore)(nil)
)

type GetListDelete interface {
	strategy.Getter
	strategy.Lister
	strategy.Deleter
}

type GetListDeleteStore struct {
	*strategy.GetAdapter
	*strategy.ListAdapter
	*strategy.DeleteAdapter
	*strategy.DestroyAdapter
	*strategy.NewAdapter
}

func (r *GetListDeleteStore) NamespaceScoped() bool {
	return r.ListAdapter.NamespaceScoped()
}

func NewGetListDelete(scheme *runtime.Scheme, getListDelete GetListDelete) *GetListDeleteStore {
	return &GetListDeleteStore{
		GetAdapter:     strategy.NewGet(getListDelete),
		ListAdapter:    strategy.NewList(getListDelete),
		DeleteAdapter:  strategy.NewDelete(scheme, getListDelete),
		DestroyAdapter: &strategy.DestroyAdapter{},
		NewAdapter:     strategy.NewNew(getListDelete),
	}
}
