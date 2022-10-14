package translation

import (
	"context"

	"github.com/acorn-io/mink/pkg/strategy"
	"github.com/acorn-io/mink/pkg/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

var _ strategy.CompleteStrategy = (*Strategy)(nil)

type Translator interface {
	FromPublicName(ctx context.Context, namespace, name string) (string, string, error)
	ListOpts(namespace string, opts storage.ListOptions) (string, storage.ListOptions)
	ToPublic(obj ...runtime.Object) []types.Object
	FromPublic(ctx context.Context, obj runtime.Object) (types.Object, error)
	NewPublic() types.Object
	NewPublicList() types.ObjectList
}

func NewTranslationStrategy(translator Translator, strategy strategy.CompleteStrategy) *Strategy {
	return &Strategy{
		strategy:   strategy,
		translator: translator,
	}
}

type Strategy struct {
	strategy   strategy.CompleteStrategy
	translator Translator
}

func (t *Strategy) toPublic(obj runtime.Object, err error, namespace, name string) (types.Object, error) {
	if err != nil {
		return nil, err
	}
	objs := t.translator.ToPublic(obj)
	for _, obj := range objs {
		if obj.GetNamespace() == namespace && obj.GetName() == name {
			return obj, nil
		}
	}
	if len(objs) > 0 {
		return objs[0], nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{}, name)
}

func (t *Strategy) Create(ctx context.Context, object types.Object) (types.Object, error) {
	newObj, err := t.translator.FromPublic(ctx, object)
	if err != nil {
		return nil, err
	}
	o, err := t.strategy.Create(ctx, newObj)
	return t.toPublic(o, err, object.GetNamespace(), object.GetName())
}

func (t *Strategy) New() types.Object {
	return t.translator.NewPublic()
}

func (t *Strategy) Get(ctx context.Context, namespace, name string) (types.Object, error) {
	newNamespace, newName, err := t.translator.FromPublicName(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	o, err := t.strategy.Get(ctx, newNamespace, newName)
	return t.toPublic(o, err, namespace, name)
}

func (t *Strategy) Update(ctx context.Context, obj types.Object) (types.Object, error) {
	newObj, err := t.translator.FromPublic(ctx, obj)
	if err != nil {
		return nil, err
	}
	o, err := t.strategy.Update(ctx, newObj)
	return t.toPublic(o, err, obj.GetNamespace(), obj.GetName())
}

func (t *Strategy) UpdateStatus(ctx context.Context, obj types.Object) (types.Object, error) {
	newObj, err := t.translator.FromPublic(ctx, obj)
	if err != nil {
		return nil, err
	}
	o, err := t.strategy.UpdateStatus(ctx, newObj)
	return t.translator.ToPublic(o)[0], err
}

func (t *Strategy) toPublicList(obj types.ObjectList) (types.ObjectList, error) {
	var (
		items      []runtime.Object
		list       = obj.(types.ObjectList)
		publicList = t.translator.NewPublicList()
	)

	err := meta.EachListItem(list, func(obj runtime.Object) error {
		items = append(items, obj)
		return nil
	})
	if err != nil {
		return nil, err
	}

	publicItems := make([]runtime.Object, 0, len(items))
	for _, obj := range t.translator.ToPublic(items...) {
		publicItems = append(publicItems, obj)
	}

	err = meta.SetList(publicList, publicItems)
	if err != nil {
		return nil, err
	}

	publicList.SetContinue(list.GetContinue())
	publicList.SetResourceVersion(list.GetResourceVersion())
	return publicList, nil
}

func (t *Strategy) List(ctx context.Context, namespace string, opts storage.ListOptions) (types.ObjectList, error) {
	namespace, opts, err := t.translateListOpts(ctx, namespace, opts)
	if err != nil {
		return nil, err
	}
	o, err := t.strategy.List(ctx, namespace, opts)
	if err != nil {
		return nil, err
	}
	return t.toPublicList(o)
}

func (t *Strategy) NewList() types.ObjectList {
	return t.translator.NewPublicList()
}

func (t *Strategy) Delete(ctx context.Context, obj types.Object) (types.Object, error) {
	newObj, err := t.translator.FromPublic(ctx, obj)
	if err != nil {
		return nil, err
	}
	o, err := t.strategy.Delete(ctx, newObj)
	return t.toPublic(o, err, obj.GetNamespace(), obj.GetName())
}

func (t *Strategy) translateListOpts(ctx context.Context, namespace string, opts storage.ListOptions) (string, storage.ListOptions, error) {
	if opts.Predicate.Field != nil {
		var err error
		opts.Predicate.Field, err = opts.Predicate.Field.Transform(func(field, value string) (newField, newValue string, err error) {
			if field == "metadata.name" {
				_, newName, err := t.translator.FromPublicName(ctx, namespace, value)
				if err != nil {
					return "", "", err
				}
				return field, newName, nil
			}
			return field, value, nil
		})
		if err != nil {
			return "", storage.ListOptions{}, err
		}
	}

	namespace, opts = t.translator.ListOpts(namespace, opts)
	return namespace, opts, nil
}

func (t *Strategy) Watch(ctx context.Context, namespace string, opts storage.ListOptions) (<-chan watch.Event, error) {
	namespace, newOpts, err := t.translateListOpts(ctx, namespace, opts)
	if err != nil {
		return nil, err
	}

	w, err := t.strategy.Watch(ctx, namespace, newOpts)
	if err != nil {
		return nil, err
	}

	result := make(chan watch.Event)
	go func() {
		defer close(result)

		for event := range w {
			switch event.Type {
			case watch.Added:
				fallthrough
			case watch.Deleted:
				fallthrough
			case watch.Modified:
				for _, obj := range t.translator.ToPublic(event.Object) {
					if ok, err := opts.Predicate.Matches(obj); err != nil {
						result <- watch.Event{
							Type:   watch.Error,
							Object: &apierrors.NewInternalError(err).ErrStatus,
						}
					} else if ok {
						event.Object = obj
						result <- event
					}
				}
			default:
				result <- event
			}
		}
	}()

	return result, nil
}

func (t *Strategy) Destroy() {
	t.strategy.Destroy()
}
