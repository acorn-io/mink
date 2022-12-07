package apigroup

import (
	"github.com/acorn-io/mink/pkg/serializer"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
)

type AddToScheme func(*runtime.Scheme) error

func ForStores(scheme AddToScheme, stores map[string]rest.Storage, groupVersion schema.GroupVersion) (*genericapiserver.APIGroupInfo, error) {
	newScheme := runtime.NewScheme()
	if err := scheme(newScheme); err != nil {
		return nil, err
	}

	for _, store := range stores {
		newScheme.AddKnownTypes(schema.GroupVersion{
			Group:   groupVersion.Group,
			Version: runtime.APIVersionInternal,
		}, store.New())
	}

	codecs := runtimeserializer.NewCodecFactory(newScheme)
	parameterCodec := runtime.NewParameterCodec(newScheme)
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(groupVersion.Group, newScheme, parameterCodec, codecs)
	apiGroupInfo.VersionedResourcesStorageMap[groupVersion.Version] = stores
	if groupVersion.Group != "" {
		apiGroupInfo.NegotiatedSerializer = serializer.NewNoProtobufSerializer(apiGroupInfo.NegotiatedSerializer)
	}
	return &apiGroupInfo, nil
}
