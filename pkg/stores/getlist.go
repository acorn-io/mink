package stores

import (
	"github.com/acorn-io/mink/pkg/strategy"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Getter   = (*GetListStore)(nil)
	_ rest.Lister   = (*GetListStore)(nil)
	_ strategy.Base = (*GetListStore)(nil)
)

type GetList interface {
	strategy.Getter
	strategy.Lister
}

type GetListStore struct {
	*strategy.GetAdapter
	*strategy.ListAdapter
	*strategy.DestroyAdapter
	*strategy.NewAdapter
}

func (r *GetListStore) NamespaceScoped() bool {
	return r.ListAdapter.NamespaceScoped()
}

func NewGetList(getList GetList) *GetListStore {
	return &GetListStore{
		GetAdapter:     strategy.NewGet(getList),
		ListAdapter:    strategy.NewList(getList),
		DestroyAdapter: &strategy.DestroyAdapter{},
		NewAdapter:     strategy.NewNew(getList),
	}
}
