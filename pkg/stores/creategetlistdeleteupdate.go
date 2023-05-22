package stores

import (
	"github.com/acorn-io/mink/pkg/strategy"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Getter             = (*CreateGetListDeleteUpdateStore)(nil)
	_ rest.Lister             = (*CreateGetListDeleteUpdateStore)(nil)
	_ rest.Updater            = (*CreateGetListDeleteUpdateStore)(nil)
	_ rest.RESTDeleteStrategy = (*CreateGetListDeleteUpdateStore)(nil)
	_ strategy.Base           = (*CreateGetListDeleteUpdateStore)(nil)
)

type CreateGetListDeleteUpdateStore struct {
	*strategy.SingularNameAdapter
	*strategy.GetAdapter
	*strategy.CreateAdapter
	*strategy.ListAdapter
	*strategy.DeleteAdapter
	*strategy.DestroyAdapter
	*strategy.UpdateAdapter
	*strategy.TableAdapter
}

func (c *CreateGetListDeleteUpdateStore) New() runtime.Object {
	return c.CreateAdapter.New()
}

func (c *CreateGetListDeleteUpdateStore) NamespaceScoped() bool {
	return c.ListAdapter.NamespaceScoped()
}
