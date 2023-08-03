package db

import (
	"fmt"

	"github.com/go-sql-driver/mysql"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	OptimisticLockErrorMsg = "the object has been modified; please apply your changes to the latest version and try again"
)

func newConflict(gvk schema.GroupVersionKind, name string, err error) error {
	return apierrors.NewConflict(
		schema.GroupResource{
			Group:    gvk.Group,
			Resource: gvk.Kind,
		}, name, err)
}

func newAlreadyExists(gvk schema.GroupVersionKind, name string) error {
	return apierrors.NewAlreadyExists(
		schema.GroupResource{
			Group:    gvk.Group,
			Resource: gvk.Kind,
		}, name)
}

func newNotFound(gvk schema.GroupVersionKind, name string) error {
	return apierrors.NewNotFound(
		schema.GroupResource{
			Group:    gvk.Group,
			Resource: gvk.Kind,
		}, name)
}

func newCompactionError(requested, current uint) error {
	return apierrors.NewResourceExpired(fmt.Sprintf("resource version %d before current compaction %d", requested, current))
}

func newResourceVersionMismatch(gvk schema.GroupVersionKind, name string) error {
	return apierrors.NewConflict(schema.GroupResource{
		Group:    gvk.Group,
		Resource: gvk.Kind,
	}, name, fmt.Errorf(OptimisticLockErrorMsg))
}

func translateDuplicateEntryErr(err error, gvk schema.GroupVersionKind, objName string) error {
	if err == nil {
		return err
	}

	if err, ok := err.(*mysql.MySQLError); ok && err.Number == 1062 { // error 1062 is a duplicate entry error
		return newConflict(gvk, objName, err)
	}
	return err
}
