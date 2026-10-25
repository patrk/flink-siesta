package probe

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestKafkaConfigOpts(t *testing.T) {
	base := KafkaConfig{BootstrapServers: []string{"b:9092"}}
	cases := []struct {
		name    string
		cfg     KafkaConfig
		wantN   int    // number of options; seed brokers is always one
		wantErr string // substring, empty for success
	}{
		{"no brokers", KafkaConfig{}, 0, "bootstrap servers are required"},
		{"plaintext default", base, 1, ""},
		{"explicit PLAINTEXT", with(base, func(c *KafkaConfig) { c.SecurityProtocol = "PLAINTEXT" }), 1, ""},
		{"unknown protocol", with(base, func(c *KafkaConfig) { c.SecurityProtocol = "KERBEROS" }), 0, "unknown security protocol"},
		{"SSL only", with(base, func(c *KafkaConfig) { c.SecurityProtocol = "SSL" }), 2, ""},
		{"SASL without credentials", with(base, func(c *KafkaConfig) { c.SecurityProtocol = "SASL_SSL"; c.SASLMechanism = "PLAIN" }), 0, "requires SASL username and password"},
		{"Confluent Cloud shape", with(base, func(c *KafkaConfig) {
			c.SecurityProtocol, c.SASLMechanism, c.SASLUsername, c.SASLPassword = "SASL_SSL", "PLAIN", "APIKEY", "secret"
		}), 3, ""},
		{"SCRAM-512 over plaintext", with(base, func(c *KafkaConfig) {
			c.SecurityProtocol, c.SASLMechanism, c.SASLUsername, c.SASLPassword = "SASL_PLAINTEXT", "scram-sha-512", "u", "p"
		}), 2, ""},
		{"unsupported mechanism", with(base, func(c *KafkaConfig) {
			c.SecurityProtocol, c.SASLMechanism, c.SASLUsername, c.SASLPassword = "SASL_SSL", "GSSAPI", "u", "p"
		}), 0, "unsupported SASL mechanism"},
		{"cert without key", with(base, func(c *KafkaConfig) { c.SecurityProtocol = "SSL"; c.TLSCertFile = "x.pem" }), 0, "must be set together"},
		{"missing CA file", with(base, func(c *KafkaConfig) { c.SecurityProtocol = "SSL"; c.TLSCAFile = "/nonexistent" }), 0, "read CA file"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts, err := c.cfg.Opts()
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want error containing %q, got %v", c.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(opts) != c.wantN {
				t.Fatalf("want %d options, got %d", c.wantN, len(opts))
			}
		})
	}
}

func TestKafkaConfigCustomCA(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, selfSignedPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := KafkaConfig{BootstrapServers: []string{"b:9093"}, SecurityProtocol: "SSL", TLSCAFile: caFile}
	tlsCfg, err := cfg.tlsConfig()
	if err != nil {
		t.Fatal(err)
	}
	if tlsCfg.RootCAs == nil {
		t.Fatal("want a root CA pool from the PEM file")
	}
	garbage := filepath.Join(dir, "garbage.pem")
	if err := os.WriteFile(garbage, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.TLSCAFile = garbage
	if _, err := cfg.tlsConfig(); err == nil || !strings.Contains(err.Error(), "no PEM certificates") {
		t.Fatalf("want a clear error for a non-PEM file, got %v", err)
	}
}

func TestKafkaConfigFromEnv(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "a:9092, b:9092")
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
	t.Setenv("KAFKA_SASL_MECHANISM", "PLAIN")
	t.Setenv("KAFKA_SASL_USERNAME", "key")
	t.Setenv("KAFKA_SASL_PASSWORD", "secret")
	c := KafkaConfigFromEnv()
	if len(c.BootstrapServers) != 2 || c.BootstrapServers[1] != "b:9092" {
		t.Fatalf("bootstrap servers not split and trimmed: %v", c.BootstrapServers)
	}
	if c.SecurityProtocol != "SASL_SSL" || c.SASLUsername != "key" {
		t.Fatalf("env not mapped: %+v", c)
	}
}

func with(c KafkaConfig, f func(*KafkaConfig)) KafkaConfig {
	f(&c)
	return c
}

// selfSignedPEM makes a throwaway CA certificate so the test does not depend on any file.
func selfSignedPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "siesta-test-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestCredentialsFromFilesAreReadEachTime(t *testing.T) {
	dir := t.TempDir()
	uf, pf := filepath.Join(dir, "username"), filepath.Join(dir, "password")
	if err := os.WriteFile(uf, []byte("key-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pf, []byte("secret-1"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := KafkaConfig{BootstrapServers: []string{"b:9092"}, SecurityProtocol: "SASL_SSL", SASLMechanism: "PLAIN", SASLUsernameFile: uf, SASLPasswordFile: pf}
	if _, err := c.Opts(); err != nil {
		t.Fatal(err)
	}
	u, p, err := c.credentials()
	if err != nil || u != "key-1" || p != "secret-1" {
		t.Fatalf("want key-1/secret-1 (trimmed), got %q/%q err=%v", u, p, err)
	}
	// rotation: the Secret's file changes, nothing restarts
	if err := os.WriteFile(pf, []byte("secret-2"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, p, _ = c.credentials(); p != "secret-2" {
		t.Fatalf("a rotated password must be read on the next authentication, got %q", p)
	}
	// a missing file is a configuration error at startup, not a silent empty password
	c.SASLPasswordFile = filepath.Join(dir, "missing")
	if _, err := c.Opts(); err == nil {
		t.Fatal("a missing credential file must fail Opts")
	}
}

// testCA is a throwaway CA that can sign a server certificate for 127.0.0.1.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T, name string) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca testCA) serverCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "broker"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// cert-manager rotates the CA bundle and the certificates in the mounted Secret while the
// controller runs. Every new connection reads the files again, so a rotation needs no restart.
func TestTLSFilesAreReadOnEveryDial(t *testing.T) {
	first, second := newTestCA(t, "first"), newTestCA(t, "second")
	var serving atomic.Pointer[tls.Certificate]
	cert := first.serverCert(t)
	serving.Store(&cert)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return serving.Load(), nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = conn.(*tls.Conn).Handshake(); _ = conn.Close() }()
		}
	}()

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caFile, first.pem, 0o600); err != nil {
		t.Fatal(err)
	}
	c := KafkaConfig{BootstrapServers: []string{ln.Addr().String()}, SecurityProtocol: "SSL", TLSCAFile: caFile}
	if _, err := c.Opts(); err != nil {
		t.Fatal(err)
	}
	dial := func() error {
		conn, err := c.dialTLS(context.Background(), "tcp", ln.Addr().String())
		if err == nil {
			conn.Close()
		}
		return err
	}
	if err := dial(); err != nil {
		t.Fatalf("a broker signed by the configured CA must be trusted: %v", err)
	}

	cert = second.serverCert(t)
	serving.Store(&cert)
	if err := dial(); err == nil {
		t.Fatal("a broker signed by a CA not in the file must not be trusted")
	}
	if err := os.WriteFile(caFile, second.pem, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := dial(); err != nil {
		t.Fatalf("the rotated CA file must be read on the next dial: %v", err)
	}
}
