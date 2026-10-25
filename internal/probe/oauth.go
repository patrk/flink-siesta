package probe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sasl/oauth"
)

// tokenSource answers each OAUTHBEARER authentication: from a file read every time, such as a
// projected ServiceAccount token the kubelet rotates, or from an OAuth token endpoint with the
// client credentials grant, the token reused until a minute before it expires.
type tokenSource struct {
	c   KafkaConfig
	now func() time.Time
	ext map[string]string

	mu      sync.Mutex
	token   string
	expires time.Time
}

// tokenRefreshBefore is how long before its expiry a fetched token is replaced.
const tokenRefreshBefore = time.Minute

func (c KafkaConfig) tokenSource() *tokenSource {
	ext, _ := parseExtensions(c.OAuthExtensions)
	return &tokenSource{c: c, now: time.Now, ext: ext}
}

// validateOAuth checks the configuration once, at startup.
func (c KafkaConfig) validateOAuth() error {
	file, endpoint := c.OAuthTokenFile != "", c.OAuthTokenEndpoint != ""
	switch {
	case file == endpoint:
		return fmt.Errorf("kafka: OAUTHBEARER needs exactly one of a token file or a token endpoint")
	case endpoint && (c.OAuthClientID == "" || (c.OAuthClientSecret == "" && c.OAuthClientSecretFile == "")):
		return fmt.Errorf("kafka: an OAUTHBEARER token endpoint needs a client id and a client secret")
	}
	if _, err := parseExtensions(c.OAuthExtensions); err != nil {
		return err
	}
	if file {
		if _, err := readTrimmed(c.OAuthTokenFile, "token file"); err != nil {
			return err
		}
	}
	if c.OAuthClientSecretFile != "" {
		if _, err := readTrimmed(c.OAuthClientSecretFile, "client secret file"); err != nil {
			return err
		}
	}
	return nil
}

func (s *tokenSource) auth(ctx context.Context) (oauth.Auth, error) {
	if s.c.OAuthTokenFile != "" {
		tok, err := readTrimmed(s.c.OAuthTokenFile, "token file")
		return oauth.Auth{Token: tok, Extensions: s.ext}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == "" || !s.now().Before(s.expires.Add(-tokenRefreshBefore)) {
		tok, ttl, err := s.fetch(ctx)
		if err != nil {
			return oauth.Auth{}, err
		}
		s.token, s.expires = tok, s.now().Add(ttl)
	}
	return oauth.Auth{Token: s.token, Extensions: s.ext}, nil
}

// fetch runs the client credentials grant, RFC 6749 section 4.4, with the client
// authenticating by HTTP Basic, as Kafka's own OAuth login callback does.
func (s *tokenSource) fetch(ctx context.Context) (string, time.Duration, error) {
	secret := s.c.OAuthClientSecret
	if s.c.OAuthClientSecretFile != "" {
		var err error
		if secret, err = readTrimmed(s.c.OAuthClientSecretFile, "client secret file"); err != nil {
			return "", 0, err
		}
	}
	form := url.Values{"grant_type": {"client_credentials"}}
	if s.c.OAuthScope != "" {
		form.Set("scope", s.c.OAuthScope)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.c.OAuthTokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(url.QueryEscape(s.c.OAuthClientID), url.QueryEscape(secret))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("kafka: token endpoint: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("kafka: token endpoint answered %s", resp.Status)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tok); err != nil || tok.AccessToken == "" {
		return "", 0, fmt.Errorf("kafka: token endpoint returned no access_token")
	}
	ttl := time.Duration(tok.ExpiresIn) * time.Second
	if ttl <= tokenRefreshBefore {
		ttl = tokenRefreshBefore + time.Second // no expiry given, or a very short one: refresh soon
	}
	return tok.AccessToken, ttl, nil
}

// parseExtensions reads "key=value,key=value", the SASL extensions some brokers need, such as
// Confluent Cloud's logicalCluster and identityPoolId.
func parseExtensions(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, kv := range splitList(s) {
		k, v, ok := strings.Cut(kv, "=")
		if k, v = strings.TrimSpace(k), strings.TrimSpace(v); !ok || k == "" || v == "" {
			return nil, fmt.Errorf("kafka: OAUTHBEARER extension %q is not key=value", kv)
		}
		out[k] = v
	}
	return out, nil
}

func readTrimmed(path, what string) (string, error) {
	b, err := os.ReadFile(path) //nolint:gosec // a path from the controller's own configuration, like the other credential files
	if err != nil {
		return "", fmt.Errorf("kafka: read %s: %w", what, err)
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		return "", fmt.Errorf("kafka: %s %s is empty", what, path)
	}
	return v, nil
}
