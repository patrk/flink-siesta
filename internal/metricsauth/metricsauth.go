// Package metricsauth lets only callers the Kubernetes API vouches for read the metrics: the
// bearer token is checked with a TokenReview, the caller's right to GET /metrics with a
// SubjectAccessReview. It is what controller-runtime's metrics filters do, without the
// k8s.io/apiserver dependency they bring, which would grow the binary by more than half.
package metricsauth

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	authnclient "k8s.io/client-go/kubernetes/typed/authentication/v1"
	authzclient "k8s.io/client-go/kubernetes/typed/authorization/v1"
	"k8s.io/client-go/rest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// remember is how long an answer for a token is reused. Prometheus scrapes every 30 seconds or
// so; without it every scrape costs two API calls.
const remember = time.Minute

// Filter is a metricsserver.FilterProvider.
func Filter(cfg *rest.Config, hc *http.Client) (metricsserver.Filter, error) {
	authn, err := authnclient.NewForConfigAndClient(cfg, hc)
	if err != nil {
		return nil, err
	}
	authz, err := authzclient.NewForConfigAndClient(cfg, hc)
	if err != nil {
		return nil, err
	}
	return New(reviews{authn: authn.TokenReviews(), authz: authz.SubjectAccessReviews()}), nil
}

// Reviewer asks the Kubernetes API about a token: who it belongs to, and whether they may GET path.
type Reviewer interface {
	Allowed(ctx context.Context, token, path string) (bool, error)
}

type reviews struct {
	authn authnclient.TokenReviewInterface
	authz authzclient.SubjectAccessReviewInterface
}

func (r reviews) Allowed(ctx context.Context, token, path string) (bool, error) {
	tr, err := r.authn.Create(ctx, &authnv1.TokenReview{Spec: authnv1.TokenReviewSpec{Token: token}}, metav1.CreateOptions{})
	if err != nil {
		return false, err
	}
	if !tr.Status.Authenticated {
		return false, nil
	}
	u := tr.Status.User
	extra := map[string]authzv1.ExtraValue{}
	for k, v := range u.Extra {
		extra[k] = authzv1.ExtraValue(v)
	}
	sar, err := r.authz.Create(ctx, &authzv1.SubjectAccessReview{Spec: authzv1.SubjectAccessReviewSpec{
		User: u.Username, UID: u.UID, Groups: u.Groups, Extra: extra,
		NonResourceAttributes: &authzv1.NonResourceAttributes{Path: path, Verb: "get"},
	}}, metav1.CreateOptions{})
	if err != nil {
		return false, err
	}
	return sar.Status.Allowed, nil
}

type answer struct {
	allowed bool
	until   time.Time
}

// New wraps the metrics handler with the review. Exposed for tests.
func New(r Reviewer) metricsserver.Filter {
	var mu sync.Mutex
	seen := map[[32]byte]answer{}
	return func(log logr.Logger, next http.Handler) (http.Handler, error) {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			token, ok := strings.CutPrefix(req.Header.Get("Authorization"), "Bearer ")
			if !ok || token == "" {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			key, now := sha256.Sum256([]byte(token+"\x00"+req.URL.Path)), time.Now()
			mu.Lock()
			a, cached := seen[key]
			mu.Unlock()
			if !cached || now.After(a.until) {
				allowed, err := r.Allowed(req.Context(), token, req.URL.Path)
				if err != nil {
					log.Error(err, "could not review a metrics request")
					http.Error(w, "Internal Server Error", http.StatusInternalServerError)
					return
				}
				a = answer{allowed: allowed, until: now.Add(remember)}
				mu.Lock()
				if len(seen) >= 1000 {
					seen = map[[32]byte]answer{}
				}
				seen[key] = a
				mu.Unlock()
			}
			if !a.allowed {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, req)
		}), nil
	}
}
