package remote

import (
	"github.com/acorn-io/mink/pkg/strategy"
	"github.com/acorn-io/mink/pkg/strategy/translation"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

func NewWithSimpleTranslation(translator translation.SimpleTranslator, pub kclient.Object, kclient kclient.WithWatch) strategy.CompleteStrategy {
	pubList := getList(pub, kclient)

	priv := translator.FromPublic(pub)
	privList := getList(priv, kclient)

	remote := NewRemote(priv, privList, kclient)
	simpleTranslator := translation.NewSimpleTranslator(translator, pub, pubList)
	return translation.NewTranslationStrategy(simpleTranslator, remote, getGVK(pub, kclient))
}

func getList(obj kclient.Object, watch kclient.WithWatch) kclient.ObjectList {
	gvk := getGVK(obj, watch)
	gvk.Kind += "List"
	objList, err := watch.Scheme().New(gvk)
	if err != nil {
		panic(err)
	}
	return objList.(kclient.ObjectList)
}

func NewWithTranslation(translator translation.Translator, obj kclient.Object, kclient kclient.WithWatch) strategy.CompleteStrategy {
	remote := NewRemote(obj, getList(obj, kclient), kclient)
	return translation.NewTranslationStrategy(translator, remote, getGVK(translator.NewPublic(), kclient))
}

func getGVK(obj kclient.Object, watch kclient.WithWatch) schema.GroupVersionKind {
	gvk, err := apiutil.GVKForObject(obj, watch.Scheme())
	if err != nil {
		panic(err)
	}
	return gvk
}
