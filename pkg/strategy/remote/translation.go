package remote

import (
	"github.com/acorn-io/mink/pkg/strategy"
	"github.com/acorn-io/mink/pkg/strategy/translation"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func NewWithTranslation(translator translation.Translator, obj kclient.Object, objList kclient.ObjectList, kclient kclient.WithWatch) strategy.CompleteStrategy {
	remote := NewRemote(obj, objList, kclient)
	return translation.NewTranslationStrategy(translator, remote)
}
