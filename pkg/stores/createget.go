package stores

import (
	"github.com/acorn-io/mink/pkg/strategy"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Getter   = (*CreateGetStore)(nil)
	_ rest.Creater  = (*CreateGetStore)(nil)
	_ strategy.Base = (*CreateGetStore)(nil)
)

type CreateGet interface {
	strategy.Getter
	strategy.Creater
}

type CreateGetStore struct {
	*strategy.CreateAdapter
	*strategy.GetAdapter
	*strategy.DestroyAdapter
	*strategy.TableAdapter
}

func NewCreateGet(scheme *runtime.Scheme, s CreateGet) *CreateGetStore {
	return &CreateGetStore{
		CreateAdapter: strategy.NewCreate(scheme, s),
		GetAdapter:    strategy.NewGet(s),
		TableAdapter:  strategy.NewTable(s),
	}
}
