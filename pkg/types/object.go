package types

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type Object kclient.Object

type ObjectList kclient.ObjectList

func MustGetListType(obj kclient.Object, scheme *runtime.Scheme) kclient.ObjectList {
	gvk := MustGetGVK(obj, scheme)
	gvk.Kind += "List"
	objList, err := scheme.New(gvk)
	if err != nil {
		panic(err)
	}
	return objList.(kclient.ObjectList)
}

func MustGetGVK(obj kclient.Object, scheme *runtime.Scheme) schema.GroupVersionKind {
	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		panic(err)
	}
	return gvk
}
