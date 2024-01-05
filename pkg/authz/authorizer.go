package authz

import (
	"context"

	"github.com/acorn-io/mink/pkg/authz/binding"
	"github.com/sirupsen/logrus"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type BindingProvider interface {
	ForUser(ctx context.Context, c kclient.Client, user user.Info) ([]binding.Binding, error)
	ForAttributes(ctx context.Context, c kclient.Client, user user.Info, attr authorizer.Attributes) ([]binding.Binding, error)
}

type BindingAuthorizer interface {
	authorizer.Authorizer
	AppendBindingProviders(provider ...BindingProvider)
	Bindings(ctx context.Context, user user.Info) ([]binding.Binding, error)
}

type Authorizer struct {
	Client    kclient.Client
	Providers []BindingProvider
}

// AppendBindingProviders adds a new binding provider to the authorizer.
func (a *Authorizer) AppendBindingProviders(providers ...BindingProvider) {
	if a != nil && len(providers) > 0 {
		a.Providers = append(a.Providers, providers...)
	}
}

// Bindings is used by steve to provide all rules for the current user.
func (a *Authorizer) Bindings(ctx context.Context, user user.Info) (result []binding.Binding, _ error) {
	for _, provider := range a.Providers {
		bindings, err := provider.ForUser(ctx, a.Client, user)
		if err != nil {
			return nil, err
		}
		for _, binding := range bindings {
			if binding.MatchesUser(user) {
				result = append(result, binding)
			}
		}
	}
	return
}

// Authorize is called by k8s.
func (a *Authorizer) Authorize(ctx context.Context, attr authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
	for _, provider := range a.Providers {
		bindings, err := provider.ForAttributes(ctx, a.Client, attr.GetUser(), attr)
		if err != nil {
			return authorizer.DecisionDeny, "error", err
		}
		for _, binding := range bindings {
			if binding.MatchesUser(attr.GetUser()) {
				for _, rule := range binding.GetRules() {
					if rule.Matches(attr) {
						return authorizer.DecisionAllow, "", nil
					}
				}
			}
		}
	}

	logrus.Debugf("Rejecting %s to %s %s", attr.GetUser().GetName(), attr.GetVerb(), attr.GetPath())
	return authorizer.DecisionDeny, "", nil
}
