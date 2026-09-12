package probe

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type KafkaConfig struct {
	BootstrapServers []string
	SecurityProtocol string // PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
	SASLMechanism    string // PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
	SASLUsername     string
	SASLPassword     string
	TLSCAFile        string // optional: PEM bundle for a private CA
	TLSCertFile      string // optional: client certificate for mTLS
	TLSKeyFile       string
	TLSInsecure      bool // skip server verification; for throwaway clusters only
}

// KafkaConfigFromEnv reads KAFKA_* variables. Missing values mean PLAINTEXT with no auth.
func KafkaConfigFromEnv() KafkaConfig {
	return KafkaConfig{
		BootstrapServers: splitList(os.Getenv("KAFKA_BOOTSTRAP_SERVERS")),
		SecurityProtocol: orDefault(os.Getenv("KAFKA_SECURITY_PROTOCOL"), "PLAINTEXT"),
		SASLMechanism:    os.Getenv("KAFKA_SASL_MECHANISM"),
		SASLUsername:     os.Getenv("KAFKA_SASL_USERNAME"),
		SASLPassword:     os.Getenv("KAFKA_SASL_PASSWORD"),
		TLSCAFile:        os.Getenv("KAFKA_TLS_CA_FILE"),
		TLSCertFile:      os.Getenv("KAFKA_TLS_CERT_FILE"),
		TLSKeyFile:       os.Getenv("KAFKA_TLS_KEY_FILE"),
		TLSInsecure:      strings.EqualFold(os.Getenv("KAFKA_TLS_INSECURE_SKIP_VERIFY"), "true"),
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

	if useSASL {
		if c.SASLUsername == "" || c.SASLPassword == "" {
			return nil, fmt.Errorf("kafka: %s requires SASL username and password", protocol)
		}
		switch strings.ToUpper(c.SASLMechanism) {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsMechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", c.SASLMechanism)
		}
	}

	if useTLS {
		tlsCfg, err := c.tlsConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	return opts, nil
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
