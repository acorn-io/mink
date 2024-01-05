package subjectaccessreview

import (
	"context"
	"strings"

	"github.com/acorn-io/mink/pkg/stores"
	"github.com/acorn-io/mink/pkg/types"
	"github.com/sirupsen/logrus"
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
)

func NewStore(authorizer Authorizer, scheme *runtime.Scheme) rest.Storage {
	strategy := &Strategy{
		Authorizer: authorizer,
	}
	return stores.NewBuilder(scheme, &authorizationv1.SubjectAccessReview{}).
		WithPrepareCreate(strategy).
		WithCreate(strategy).Build()
}

type Strategy struct {
	Authorizer Authorizer
}

type Authorizer interface {
	Authorize(ctx context.Context, attr authorizer.Attributes) (authorized authorizer.Decision, reason string, err error)
}

func (s *Strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	sar := obj.(*authorizationv1.SubjectAccessReview)
	if sar.Name == "" && sar.GenerateName == "" {
		sar.GenerateName = "sar-"
		sar.Name = names.SimpleNameGenerator.GenerateName(sar.GenerateName)
	}
}

func (s *Strategy) NamespaceScoped() bool {
	return false
}

func (s *Strategy) Create(ctx context.Context, object types.Object) (types.Object, error) {
	var (
		review   = object.(*authorizationv1.SubjectAccessReview)
		decision = authorizer.DecisionDeny
	)

	if autoApprove(review) {
		logrus.Debugf("SubjectAccessReview '%s' #%s: auto approved", review.GetName(), review.GetUID())
		decision = authorizer.DecisionAllow
	} else if s.Authorizer != nil && review.Spec.NonResourceAttributes == nil && review.Spec.ResourceAttributes != nil {
		user := &user.DefaultInfo{
			Name:   review.Spec.User,
			UID:    review.Spec.UID,
			Groups: review.Spec.Groups,
			Extra:  map[string][]string{},
		}
		for k, v := range review.Spec.Extra {
			user.Extra[k] = v
		}

		var (
			reason string
			err    error
			record = authorizer.AttributesRecord{
				User:            user,
				Verb:            review.Spec.ResourceAttributes.Verb,
				Namespace:       review.Spec.ResourceAttributes.Namespace,
				APIGroup:        review.Spec.ResourceAttributes.Group,
				Resource:        review.Spec.ResourceAttributes.Resource,
				Subresource:     review.Spec.ResourceAttributes.Subresource,
				Name:            review.Spec.ResourceAttributes.Name,
				ResourceRequest: true,
			}
		)

		decision, reason, err = s.Authorizer.Authorize(ctx, record)
		if err != nil {
			decision = authorizer.DecisionDeny
			review.Status.EvaluationError = err.Error()
		}
		review.Status.Reason = reason
	}

	if decision == authorizer.DecisionAllow {
		review.Status.Allowed = true
		review.Status.Denied = false
	} else if decision == authorizer.DecisionDeny {
		review.Status.Allowed = false
		review.Status.Denied = true
	}

	return review, nil
}

func (s *Strategy) New() types.Object {
	return &authorizationv1.SubjectAccessReview{}
}

// autoApprove is a hack to auto approve certain requests for simplicity
func autoApprove(review *authorizationv1.SubjectAccessReview) bool {
	if strings.HasPrefix(review.Spec.User, "image://") &&
		review.Spec.ResourceAttributes != nil &&
		review.Spec.ResourceAttributes.Group == "api.acorn.io" &&
		review.Spec.ResourceAttributes.Resource == "events" &&
		review.Spec.ResourceAttributes.Verb == "create" &&
		review.Spec.ResourceAttributes.Namespace != "" {
		// This is just a hack to auto approve event creation for all images
		return true
	}
	return false
}
