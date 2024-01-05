package binding

import (
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

const (
	all = "*"
)

var (
	All               = []string{all}
	AllSet            = sets.New(All...)
	DefaultReadVerbs  = []string{"get", "list", "watch"}
	DefaultWriteVerbs = []string{"get", "list", "watch", "create", "update", "delete", "patch"}
)

type Binding interface {
	MatchesUser(user user.Info) bool
	GetUsers() sets.Set[string]
	GetGroups() sets.Set[string]
	GetRules() []Rule
	GetID() string
}

type DefaultBinding struct {
	Name   string
	Users  sets.Set[string]
	Groups sets.Set[string]
	Rules  []Rule
}

func (b *DefaultBinding) GetUsers() sets.Set[string] {
	return b.Users
}

func (b *DefaultBinding) GetGroups() sets.Set[string] {
	return b.Groups
}

func (b *DefaultBinding) GetRules() []Rule {
	return b.Rules
}

func (b *DefaultBinding) GetID() string {
	return b.Name
}

func (b *DefaultBinding) MatchesUser(user user.Info) bool {
	userMatch := b.Users.Len() == 0
	if !userMatch {
		userMatch = b.Users.Has(user.GetName())
	}
	if !userMatch {
		return false
	}

	groupMatch := b.Groups.Len() == 0
	for _, group := range user.GetGroups() {
		if b.Groups.Has(group) {
			groupMatch = true
			break
		}
	}
	return groupMatch
}

type Rule interface {
	Matches(attr authorizer.Attributes) bool
	GetNamespaces() []string
	GetAPIGroups() []string
	GetResources() []string
	GetSubResources() []string
	GetResourceNames() []string
	GetVerbs() []string
	GetPaths() []string
}

type DefaultRule struct {
	Namespaces    []string
	APIGroups     []string
	Resources     []string
	SubResources  []string
	ResourceNames []string
	Verbs         []string
	Paths         []string
}

func (r *DefaultRule) GetNamespaces() []string {
	return r.Namespaces
}

func (r *DefaultRule) GetAPIGroups() []string {
	return r.APIGroups
}

func (r *DefaultRule) GetResources() []string {
	return r.Resources
}

func (r *DefaultRule) GetSubResources() []string {
	return r.SubResources
}

func (r *DefaultRule) GetResourceNames() []string {
	return r.ResourceNames
}

func (r *DefaultRule) GetVerbs() []string {
	return r.Verbs
}

func (r *DefaultRule) GetPaths() []string {
	return r.Paths
}

func Matches(str string, allowed []string) bool {
	for _, allow := range allowed {
		if allow == all || str == allow {
			return true
		}
		if strings.HasSuffix(allow, all) && strings.HasPrefix(str, allow[:len(allow)-len(all)]) {
			return true
		}
	}

	return false
}

func (r *DefaultRule) Matches(attr authorizer.Attributes) bool {
	if !attr.IsResourceRequest() {
		return Matches(attr.GetPath(), r.Paths)
	}
	if len(r.SubResources) > 0 && !Matches(attr.GetSubresource(), r.SubResources) {
		return false
	}
	if len(r.SubResources) == 0 && len(attr.GetSubresource()) > 0 && !Matches(all, r.Resources) {
		return false
	}
	if len(r.ResourceNames) > 0 && !Matches(attr.GetName(), r.ResourceNames) {
		return false
	}
	return Matches(attr.GetNamespace(), r.Namespaces) &&
		Matches(attr.GetVerb(), r.Verbs) &&
		Matches(attr.GetAPIGroup(), r.APIGroups) &&
		Matches(attr.GetResource(), r.Resources)
}

type forUser struct {
	Binding
	username string
}

func (f *forUser) GetID() string {
	return f.Binding.GetID() + " user:" + f.username
}

func (f *forUser) GetUsers() sets.Set[string] {
	return sets.New[string](f.username)
}

func (f *forUser) GetGroups() sets.Set[string] {
	return nil
}

func (f *forUser) MatchesUser(user user.Info) bool {
	return f.username == user.GetName()
}

// ForUser will create a new binding that will match just this user and will ignore the MatchUser behavior of the passed
// in binding
func ForUser(username string, binding Binding) Binding {
	return &forUser{
		Binding:  binding,
		username: username,
	}
}

type forNamespace struct {
	Rule
	namespace string
}

func (f *forNamespace) GetNamespaces() []string {
	return []string{f.namespace}
}

func (f *forNamespace) Matches(attr authorizer.Attributes) bool {
	if !Matches(attr.GetNamespace(), []string{f.namespace}) {
		return false
	}
	return f.Rule.Matches(attr)
}

// ForNamespace will augment to existing rule to only match the specified namespace. If the passed in rule does not
// allow all namespace or the specified namespace, the augmented rule will fail to match any attributes.
func ForNamespace(namespace string, rule Rule) Rule {
	return &forNamespace{
		Rule:      rule,
		namespace: namespace,
	}
}

// ForNamespaceBinding will augment to existing rule to only match the specified namespace. If the passed in rule does not
// allow all namespace or the specified namespace, the augmented rule will fail to match any attributes.
func ForNamespaceBinding(namespace string, binding Binding) Binding {
	return &forNamespaceBinding{
		Binding:   binding,
		namespace: namespace,
	}
}

type forNamespaceBinding struct {
	Binding
	namespace string
}

func (f *forNamespaceBinding) GetID() string {
	return f.Binding.GetID() + " namespace:" + f.namespace
}

func (f *forNamespaceBinding) GetRules() []Rule {
	rules := f.Binding.GetRules()
	result := make([]Rule, 0, len(rules))
	for _, rule := range rules {
		result = append(result, ForNamespace(f.namespace, rule))
	}
	return result
}
