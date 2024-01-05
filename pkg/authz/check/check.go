package check

import (
	"context"
	"errors"

	v1 "k8s.io/api/authorization/v1"
	"k8s.io/apiserver/pkg/endpoints/request"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func PopulateUser(ctx context.Context, sar *v1.SubjectAccessReview) {
	user, ok := request.UserFrom(ctx)
	if !ok {
		return
	}

	sar.Spec.User = user.GetName()
	sar.Spec.UID = user.GetUID()
	sar.Spec.Groups = user.GetGroups()
	sar.Spec.Extra = map[string]v1.ExtraValue{}
	for k, extra := range sar.Spec.Extra {
		sar.Spec.Extra[k] = extra
	}
}

func Can(ctx context.Context, c kclient.Client, attr *v1.ResourceAttributes) (bool, string, error) {
	sar := &v1.SubjectAccessReview{
		Spec: v1.SubjectAccessReviewSpec{
			ResourceAttributes: attr,
		},
	}
	PopulateUser(ctx, sar)
	err := c.Create(ctx, sar)
	if sar.Status.EvaluationError != "" {
		return false, "", errors.New(sar.Status.EvaluationError)
	}
	return sar.Status.Allowed, sar.Status.Reason, err
}
