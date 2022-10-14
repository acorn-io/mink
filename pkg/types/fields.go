package types

import (
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type Fields interface {
	fields.Fields
	FieldNames() []string
}

func AddKnownTypesWithFieldConversion(gv schema.GroupVersion, types ...Object) {

}
