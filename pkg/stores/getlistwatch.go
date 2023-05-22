package stores

import (
	"github.com/acorn-io/mink/pkg/strategy"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Getter   = (*GetListWatchStore)(nil)
	_ rest.Lister   = (*GetListWatchStore)(nil)
	_ rest.Watcher  = (*GetListWatchStore)(nil)
	_ strategy.Base = (*GetListWatchStore)(nil)
)

type GetListWatchStore struct {
	*strategy.SingularNameAdapter
	*strategy.NewAdapter
	*strategy.GetAdapter
	*strategy.ListAdapter
	*strategy.WatchAdapter
	*strategy.DestroyAdapter
	*strategy.TableAdapter
}

func (r *GetListWatchStore) NamespaceScoped() bool {
	return r.ListAdapter.NamespaceScoped()
}
