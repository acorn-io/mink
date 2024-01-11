package brent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/acorn-io/brent/pkg/accesscontrol"
	"github.com/acorn-io/mink/pkg/authz"
	"github.com/acorn-io/mink/pkg/authz/binding"
	"github.com/sirupsen/logrus"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

type AccessSetLookup struct {
	authorizer authz.BindingAuthorizer
}

func (a *AccessSetLookup) AccessFor(user user.Info) *accesscontrol.AccessSet {
	id := sha256.New()
	as := &accesscontrol.AccessSet{}

	bindings, err := a.authorizer.Bindings(context.TODO(), user)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"user": user.GetName(),
			"err":  err,
		}).Error("Failed to determine bindings for user")
		return &accesscontrol.AccessSet{
			ID: "err:" + err.Error(),
		}
	}

	for _, binding := range bindings {
		id.Write([]byte(binding.GetID()))
		id.Write([]byte{'\x00'})
		add(as, binding)
	}

	digest := id.Sum(nil)
	as.ID = hex.EncodeToString(digest[:])
	return as
}

func add(as *accesscontrol.AccessSet, b binding.Binding) {
	for _, rule := range b.GetRules() {
		names := rule.GetResourceNames()
		if len(names) == 0 {
			names = binding.All
		}

		for _, namespace := range rule.GetNamespaces() {
			for _, apiGroup := range rule.GetAPIGroups() {
				for _, resource := range rule.GetResources() {
					for _, verb := range rule.GetVerbs() {
						for _, name := range names {
							as.Add(verb, schema2.GroupResource{
								Group:    apiGroup,
								Resource: resource,
							}, accesscontrol.Access{
								Namespace:    namespace,
								ResourceName: name,
							})
						}
					}
				}
			}
		}
	}
}
