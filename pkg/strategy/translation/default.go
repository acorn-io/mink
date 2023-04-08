package translation

import (
	"context"

	"github.com/acorn-io/mink/pkg/strategy"
	mtypes "github.com/acorn-io/mink/pkg/types"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type SimpleTranslator interface {
	FromPublic(obj mtypes.Object) mtypes.Object
	ToPublic(obj mtypes.Object) mtypes.Object
}

func getListType(obj kclient.Object, scheme *runtime.Scheme) kclient.ObjectList {
	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		panic(err)
	}
	gvk.Kind += "List"
	objList, err := scheme.New(gvk)
	if err != nil {
		panic(err)
	}
	return objList.(kclient.ObjectList)
}

func NewSimpleTranslationStrategy(translator SimpleTranslator, strategy strategy.CompleteStrategy) strategy.CompleteStrategy {
	pubType := translator.ToPublic(strategy.New())
	return NewTranslationStrategy(NewSimpleTranslator(translator, pubType, strategy.Scheme()), strategy)
}

func NewSimpleTranslator(translator SimpleTranslator, pubType mtypes.Object, scheme *runtime.Scheme) Translator {
	return &simpleTranslator{
		obj:        pubType,
		objList:    getListType(pubType, scheme),
		translator: translator,
	}
}

type simpleTranslator struct {
	obj        mtypes.Object
	objList    mtypes.ObjectList
	translator SimpleTranslator
}

func (s *simpleTranslator) FromPublicName(ctx context.Context, namespace, name string) (string, string, error) {
	return namespace, name, nil
}

func (s *simpleTranslator) ListOpts(ctx context.Context, namespace string, opts storage.ListOptions) (string, storage.ListOptions, error) {
	return namespace, opts, nil
}

func (s *simpleTranslator) NewPublic() mtypes.Object {
	return s.obj.DeepCopyObject().(mtypes.Object)
}

func (s *simpleTranslator) NewPublicList() mtypes.ObjectList {
	return s.objList.DeepCopyObject().(mtypes.ObjectList)
}

func (s *simpleTranslator) FromPublic(ctx context.Context, obj runtime.Object) (result mtypes.Object, _ error) {
	return s.translator.FromPublic(obj.(mtypes.Object)), nil
}

func (s *simpleTranslator) ToPublic(ctx context.Context, objs ...runtime.Object) (result []mtypes.Object, _ error) {
	result = make([]mtypes.Object, 0, len(objs))
	for _, obj := range objs {
		result = append(result, s.translator.ToPublic(obj.(mtypes.Object)))
	}
	return
}
