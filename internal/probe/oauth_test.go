package probe

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A projected ServiceAccount token, or any file a sidecar keeps fresh: read on every
// authentication, so the kubelet's rotation needs nothing from the controller.
func TestOAuthTokenFileIsReadOnEveryAuthentication(t *testing.T) {
	f := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(f, []byte("jwt-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := KafkaConfig{BootstrapServers: []string{"b:9092"}, SecurityProtocol: "SASL_SSL", SASLMechanism: "OAUTHBEARER",
		OAuthTokenFile: f, OAuthExtensions: "logicalCluster=lkc-1, identityPoolId=pool-2"}
	if _, err := c.Opts(); err != nil {
		t.Fatal(err)
	}
	src := c.tokenSource().auth
	auth, err := src(context.Background())
	if err != nil || auth.Token != "jwt-1" {
		t.Fatalf("want jwt-1, got %q %v", auth.Token, err)
	}
	if auth.Extensions["logicalCluster"] != "lkc-1" || auth.Extensions["identityPoolId"] != "pool-2" {
		t.Fatalf("extensions = %v", auth.Extensions)
	}
	if err := os.WriteFile(f, []byte("jwt-2"), 0o600); err != nil {
		t.Fatal(err)
	}
	if auth, _ = src(context.Background()); auth.Token != "jwt-2" {
		t.Fatalf("a rotated token must be read on the next authentication, got %q", auth.Token)
	}
}

// The client credentials grant, as Kafka's own sasl.oauthbearer.token.endpoint.url does it:
// the token is fetched once and reused until shortly before it expires.
func TestOAuthClientCredentialsAreCachedUntilNearExpiry(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		user, pass, ok := r.BasicAuth()
		if err := r.ParseForm(); err != nil || !ok || user != "siesta" || pass != "s3cret" ||
			r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("scope") != "kafka" {
			http.Error(w, "bad request", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": fmt.Sprintf("tok-%d", calls.Load()), "token_type": "Bearer", "expires_in": 3600})
	}))
	defer srv.Close()
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := KafkaConfig{BootstrapServers: []string{"b:9092"}, SecurityProtocol: "SASL_SSL", SASLMechanism: "OAUTHBEARER",
		OAuthTokenEndpoint: srv.URL, OAuthClientID: "siesta", OAuthClientSecretFile: secret, OAuthScope: "kafka"}
	if _, err := c.Opts(); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	ts := c.tokenSource()
	ts.now = func() time.Time { return now }
	src := ts.auth
	for range 3 {
		if auth, err := src(context.Background()); err != nil || auth.Token != "tok-1" {
			t.Fatalf("want the cached tok-1, got %q %v", auth.Token, err)
		}
	}
	now = now.Add(59 * time.Minute) // within the last minute of the token's life
	if auth, err := src(context.Background()); err != nil || auth.Token != "tok-2" {
		t.Fatalf("a token about to expire must be fetched again, got %q %v", auth.Token, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("want 2 token requests, got %d", calls.Load())
	}

	c.OAuthClientID = "someone-else"
	if _, err := c.tokenSource().auth(context.Background()); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("a refused token request must be an error naming the status, got %v", err)
	}
}

func TestOAuthConfigIsValidated(t *testing.T) {
	base := KafkaConfig{BootstrapServers: []string{"b:9092"}, SecurityProtocol: "SASL_SSL", SASLMechanism: "OAUTHBEARER"}
	for name, c := range map[string]KafkaConfig{
		"neither token file nor endpoint": base,
		"both token file and endpoint":    with(base, func(c *KafkaConfig) { c.OAuthTokenFile, c.OAuthTokenEndpoint = "/t", "https://idp/token" }),
		"endpoint without client":         with(base, func(c *KafkaConfig) { c.OAuthTokenEndpoint = "https://idp/token" }),
		"missing token file":              with(base, func(c *KafkaConfig) { c.OAuthTokenFile = "/nonexistent" }),
		"bad extension": with(base, func(c *KafkaConfig) {
			c.OAuthTokenEndpoint, c.OAuthClientID, c.OAuthClientSecret, c.OAuthExtensions = "https://idp/token", "id", "s", "novalue"
		}),
	} {
		if _, err := c.Opts(); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}
