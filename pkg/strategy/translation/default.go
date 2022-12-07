package translation

import (
	"context"
	"strings"

	mtypes "github.com/acorn-io/mink/pkg/types"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage"
)

func NewDefaultTranslator(obj mtypes.Object, objList mtypes.ObjectList) *DefaultTranslator {
	return &DefaultTranslator{
		obj:     obj,
		objList: objList,
	}
}

type DefaultTranslator struct {
	obj     mtypes.Object
	objList mtypes.ObjectList
}

func (s *DefaultTranslator) FromPublicName(ctx context.Context, namespace, name string) (string, string, error) {
	return namespace, name, nil
}

func (s *DefaultTranslator) ListOpts(namespace string, opts storage.ListOptions) (string, storage.ListOptions) {
	return namespace, opts
}

func (s *DefaultTranslator) NewPublic() mtypes.Object {
	return s.obj.DeepCopyObject().(mtypes.Object)
}

func (s *DefaultTranslator) NewPublicList() mtypes.ObjectList {
	return s.objList.DeepCopyObject().(mtypes.ObjectList)
}

func (s *DefaultTranslator) FromPublic(ctx context.Context, obj runtime.Object) (result mtypes.Object, _ error) {
	kobj := obj.(mtypes.Object)
	kobj.SetUID(types.UID(strings.TrimSuffix(string(kobj.GetUID()), "-a")))
	return kobj, nil
}

func (s *DefaultTranslator) ToPublic(objs ...runtime.Object) (result []mtypes.Object) {
	result = make([]mtypes.Object, 0, len(objs))
	for _, obj := range objs {
		kobj := obj.(mtypes.Object)
		kobj.SetUID(kobj.GetUID() + "-a")
		result = append(result, kobj)
	}
	return
}
