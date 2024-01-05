package authz

import (
	"context"

	"github.com/acorn-io/mink/pkg/authz/binding"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

func NewAllowAll() BindingAuthorizer {
	return allowAll{}
}

type allowAll struct {
}

func (a allowAll) Authorize(ctx context.Context, attr authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
	return authorizer.DecisionAllow, "", nil
}

func (a allowAll) AppendBindingProviders(provider ...BindingProvider) {
}

func (a allowAll) Bindings(ctx context.Context, user user.Info) ([]binding.Binding, error) {
	return []binding.Binding{
		&binding.DefaultBinding{
			Name:   "allow-all",
			Users:  binding.AllSet,
			Groups: binding.AllSet,
			Rules: []binding.Rule{
				&binding.DefaultRule{
					Namespaces:   binding.All,
					APIGroups:    binding.All,
					Resources:    binding.All,
					SubResources: binding.All,
					Verbs:        binding.All,
				},
				&binding.DefaultRule{
					Verbs: binding.All,
					Paths: binding.All,
				},
			},
		},
	}, nil
}
