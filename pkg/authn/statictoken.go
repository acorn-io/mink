package authn

import (
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

type StaticToken struct {
	token    string
	userName string
	groups   []string
}

func NewStaticToken(userName, adminToken string, groups ...string) *StaticToken {
	return &StaticToken{
		userName: userName,
		groups:   groups,
		token:    adminToken,
	}
}

func (a *StaticToken) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	t, ok := GetBearerToken(req)
	if !ok {
		return nil, false, nil
	}

	resp := &authenticator.Response{}
	if t == a.token {
		resp.User = &user.DefaultInfo{
			Name:   a.userName,
			UID:    a.userName,
			Groups: a.groups,
		}
		logrus.Debugf("Authenticated %s", resp.User.GetName())
		// Delete header, not needed anymore
		req.Header.Del("Authorization")
		return resp, true, nil
	}

	return nil, false, nil
}

func GetBearerToken(req *http.Request) (string, bool) {
	auth := req.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return "", false
	}

	token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	if token == "" {
		return "", false
	}
	return token, true
}
