package strategy

import (
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type SingularNameAdapter struct {
	Object runtime.Object
	Scheme *runtime.Scheme
}

func NewSingularNameAdapter(obj runtime.Object, scheme *runtime.Scheme) *SingularNameAdapter {
	return &SingularNameAdapter{
		Object: obj,
		Scheme: scheme,
	}
}

func (s *SingularNameAdapter) GetSingularName() string {
	name, err := apiutil.GVKForObject(s.Object, s.Scheme)
	if err != nil {
		panic(err)
	}
	return strings.ToLower(name.Kind)
}
