package metricsauth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/go-logr/logr"
	authnv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Against a real API server: a ServiceAccount token bound to get /metrics is served, one
// without the binding is forbidden, and a request without a valid token is turned away.
func TestOnlyCallersTheAPIServerAuthorizesReadMetrics(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run make envtest first")
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	c, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(c.Create(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "metrics-reader"},
		Rules: []rbacv1.PolicyRule{{NonResourceURLs: []string{"/metrics"}, Verbs: []string{"get"}}}}))
	must(c.Create(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "prometheus-metrics"},
		RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "metrics-reader"},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "prometheus", Namespace: "default"}}}))
	token := func(name string) string {
		t.Helper()
		sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
		must(c.Create(ctx, sa))
		tr := &authnv1.TokenRequest{Spec: authnv1.TokenRequestSpec{}}
		must(c.SubResource("token").Create(ctx, sa, tr))
		return tr.Status.Token
	}
	prometheus, stranger := token("prometheus"), token("stranger")

	filter, err := Filter(cfg, mustHTTPClient(t, cfg))
	must(err)
	h, err := filter(logr.Discard(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	must(err)
	get := func(bearer string) int {
		req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w.Code
	}
	for _, c := range []struct {
		name, bearer string
		want         int
	}{
		{"bound ServiceAccount", prometheus, http.StatusOK},
		{"bound ServiceAccount, answered from memory", prometheus, http.StatusOK},
		{"ServiceAccount without the binding", stranger, http.StatusForbidden},
		{"a token the API server does not know", "not-a-token", http.StatusForbidden},
		{"no token", "", http.StatusUnauthorized},
	} {
		if got := get(c.bearer); got != c.want {
			t.Errorf("%s: want %d, got %d", c.name, c.want, got)
		}
	}
}

func mustHTTPClient(t *testing.T, cfg *rest.Config) *http.Client {
	t.Helper()
	hc, err := rest.HTTPClientFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return hc
}
