package probe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type KafkaConfig struct {
	BootstrapServers []string
	SecurityProtocol string // PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
	SASLMechanism    string // PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | OAUTHBEARER
	SASLUsername     string
	SASLPassword     string
	SASLUsernameFile string // if set, read on every authentication: rotation needs no restart
	SASLPasswordFile string
	TLSCAFile        string // optional: PEM bundle for a private CA
	TLSCertFile      string // optional: client certificate for mTLS
	TLSKeyFile       string
	TLSInsecure      bool // skip server verification; for throwaway clusters only

	// OAUTHBEARER: a token file read on every authentication, or a token endpoint for the
	// client credentials grant. Extensions are "key=value,..." pairs sent with the token.
	OAuthTokenFile        string
	OAuthTokenEndpoint    string
	OAuthClientID         string
	OAuthClientSecret     string
	OAuthClientSecretFile string
	OAuthScope            string
	OAuthExtensions       string
}

// KafkaConfigFromEnv reads KAFKA_* variables. Missing values mean PLAINTEXT with no auth.
func KafkaConfigFromEnv() KafkaConfig {
	return KafkaConfig{
		BootstrapServers: splitList(os.Getenv("KAFKA_BOOTSTRAP_SERVERS")),
		SecurityProtocol: orDefault(os.Getenv("KAFKA_SECURITY_PROTOCOL"), "PLAINTEXT"),
		SASLMechanism:    os.Getenv("KAFKA_SASL_MECHANISM"),
		SASLUsername:     os.Getenv("KAFKA_SASL_USERNAME"),
		SASLPassword:     os.Getenv("KAFKA_SASL_PASSWORD"),
		SASLUsernameFile: os.Getenv("KAFKA_SASL_USERNAME_FILE"),
		SASLPasswordFile: os.Getenv("KAFKA_SASL_PASSWORD_FILE"),
		TLSCAFile:        os.Getenv("KAFKA_TLS_CA_FILE"),
		TLSCertFile:      os.Getenv("KAFKA_TLS_CERT_FILE"),
		TLSKeyFile:       os.Getenv("KAFKA_TLS_KEY_FILE"),
		TLSInsecure:      strings.EqualFold(os.Getenv("KAFKA_TLS_INSECURE_SKIP_VERIFY"), "true"),

		OAuthTokenFile:        os.Getenv("KAFKA_SASL_OAUTHBEARER_TOKEN_FILE"),
		OAuthTokenEndpoint:    os.Getenv("KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT"),
		OAuthClientID:         os.Getenv("KAFKA_SASL_OAUTHBEARER_CLIENT_ID"),
		OAuthClientSecret:     os.Getenv("KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET"),
		OAuthClientSecretFile: os.Getenv("KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET_FILE"),
		OAuthScope:            os.Getenv("KAFKA_SASL_OAUTHBEARER_SCOPE"),
		OAuthExtensions:       os.Getenv("KAFKA_SASL_OAUTHBEARER_EXTENSIONS"),
	}
}

// Opts validates the config and turns it into franz-go client options.
func (c KafkaConfig) Opts() ([]kgo.Opt, error) {
	if len(c.BootstrapServers) == 0 {
		return nil, fmt.Errorf("kafka: bootstrap servers are required")
	}
	opts := []kgo.Opt{kgo.SeedBrokers(c.BootstrapServers...)}

	protocol := strings.ToUpper(orDefault(c.SecurityProtocol, "PLAINTEXT"))
	useTLS := protocol == "SSL" || protocol == "SASL_SSL"
	useSASL := protocol == "SASL_PLAINTEXT" || protocol == "SASL_SSL"
	if !useTLS && !useSASL && protocol != "PLAINTEXT" {
		return nil, fmt.Errorf("kafka: unknown security protocol %q", c.SecurityProtocol)
	}

	if useSASL && strings.EqualFold(c.SASLMechanism, "OAUTHBEARER") {
		if err := c.validateOAuth(); err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(oauth.Oauth(c.tokenSource().auth)))
	} else if useSASL {
		fromFiles := c.SASLUsernameFile != "" || c.SASLPasswordFile != ""
		if !fromFiles && (c.SASLUsername == "" || c.SASLPassword == "") {
			return nil, fmt.Errorf("kafka: %s requires SASL username and password", protocol)
		}
		if fromFiles {
			if _, _, err := c.credentials(); err != nil {
				return nil, err
			}
		}
		// The credential loader runs on every new connection's authentication. With files, a
		// rotated Secret is picked up by the next connection, with no restart and no polling.
		switch strings.ToUpper(c.SASLMechanism) {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
				u, p, err := c.credentials()
				return plain.Auth{User: u, Pass: p}, err
			})))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Sha256(func(context.Context) (scram.Auth, error) {
				u, p, err := c.credentials()
				return scram.Auth{User: u, Pass: p}, err
			})))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Sha512(func(context.Context) (scram.Auth, error) {
				u, p, err := c.credentials()
				return scram.Auth{User: u, Pass: p}, err
			})))
		default:
			return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER)", c.SASLMechanism)
		}
	}

	if useTLS {
		// Checked once here so a broken mount fails at startup; dialTLS reads the files again on
		// every new connection, so a CA or certificate rotated in the Secret needs no restart.
		if _, err := c.tlsConfig(); err != nil {
			return nil, err
		}
		opts = append(opts, kgo.Dialer(c.dialTLS))
	}
	return opts, nil
}

// credentials returns the current username and password, from files when configured.
func (c KafkaConfig) credentials() (user, pass string, err error) {
	user, pass = c.SASLUsername, c.SASLPassword
	if c.SASLUsernameFile != "" {
		b, err := os.ReadFile(c.SASLUsernameFile)
		if err != nil {
			return "", "", fmt.Errorf("kafka: read username file: %w", err)
		}
		user = strings.TrimSpace(string(b))
	}
	if c.SASLPasswordFile != "" {
		b, err := os.ReadFile(c.SASLPasswordFile)
		if err != nil {
			return "", "", fmt.Errorf("kafka: read password file: %w", err)
		}
		pass = strings.TrimSpace(string(b))
	}
	if user == "" || pass == "" {
		return "", "", fmt.Errorf("kafka: SASL username or password is empty")
	}
	return user, pass, nil
}

// dialTLS opens a TLS connection with the CA, certificate and key read from their files now.
func (c KafkaConfig) dialTLS(ctx context.Context, network, host string) (net.Conn, error) {
	cfg, err := c.tlsConfig()
	if err != nil {
		return nil, err
	}
	if cfg.ServerName == "" {
		if h, _, err := net.SplitHostPort(host); err == nil {
			cfg.ServerName = h
		}
	}
	d := tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: cfg}
	return d.DialContext(ctx, network, host)
}

func (c KafkaConfig) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.TLSInsecure {
		cfg.InsecureSkipVerify = true // explicit opt-in via KAFKA_TLS_INSECURE_SKIP_VERIFY, dev clusters only
	}
	if c.TLSCAFile != "" {
		pem, err := os.ReadFile(c.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafka: CA file %s contains no PEM certificates", c.TLSCAFile)
		}
		cfg.RootCAs = pool
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return nil, fmt.Errorf("kafka: client certificate and key must be set together")
	}
	if c.TLSCertFile != "" {
		cert, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func orDefault(s, dflt string) string {
	if s == "" {
		return dflt
	}
	return s
}
