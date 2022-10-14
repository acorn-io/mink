package stores

import (
	"github.com/acorn-io/mink/pkg/strategy"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Getter   = (*GetOnlyStore)(nil)
	_ strategy.Base = (*GetOnlyStore)(nil)
)

type GetOnly interface {
	strategy.Getter
	strategy.Newer
}

type GetOnlyStore struct {
	*strategy.GetAdapter
	*strategy.NewAdapter
	*strategy.DestroyAdapter
	*strategy.ScoperAdapter
	*strategy.TableAdapter
}

func NewGetOnly(getOnly GetOnly) *GetOnlyStore {
	return &GetOnlyStore{
		GetAdapter:    strategy.NewGet(getOnly),
		NewAdapter:    strategy.NewNew(getOnly),
		ScoperAdapter: strategy.NewScoper(getOnly),
		TableAdapter:  strategy.NewTable(getOnly),
	}
}
