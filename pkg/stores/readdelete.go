package stores

import (
	"github.com/acorn-io/mink/pkg/strategy"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Getter             = (*ReadDeleteStore)(nil)
	_ rest.Lister             = (*ReadDeleteStore)(nil)
	_ rest.Watcher            = (*ReadDeleteStore)(nil)
	_ strategy.Base           = (*ReadDeleteStore)(nil)
	_ rest.RESTDeleteStrategy = (*ReadDeleteStore)(nil)
)

type ReadDelete interface {
	strategy.Getter
	strategy.Lister
	strategy.Watcher
	strategy.Deleter
}

type ReadDeleteStore struct {
	*strategy.GetAdapter
	*strategy.ListAdapter
	*strategy.WatchAdapter
	*strategy.DeleteAdapter
	*strategy.DestroyAdapter
	*strategy.NewAdapter
}

func (r *ReadDeleteStore) NamespaceScoped() bool {
	return r.ListAdapter.NamespaceScoped()
}

func NewReadDelete(scheme *runtime.Scheme, readDelete ReadDelete) *ReadDeleteStore {
	return &ReadDeleteStore{
		GetAdapter:     strategy.NewGet(readDelete),
		ListAdapter:    strategy.NewList(readDelete),
		WatchAdapter:   strategy.NewWatch(readDelete),
		DeleteAdapter:  strategy.NewDelete(scheme, readDelete),
		DestroyAdapter: &strategy.DestroyAdapter{},
		NewAdapter:     strategy.NewNew(readDelete),
	}
}
