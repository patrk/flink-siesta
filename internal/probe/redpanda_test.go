//go:build integration

package probe

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A second Kafka implementation, and the only way to exercise SASL and TLS against a real
// broker without building one: Redpanda's module can enable both. franz-go is by the same
// author as Redpanda's Kafka layer, so this is also the pairing most likely to hide nothing.
func TestKafkaProbeWithSASLAndTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := testcontainers.NewDockerProvider(); err != nil {
		t.Skipf("docker not available: %v", err)
	}

	certPEM, keyPEM := serverCert(t)
	c, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v26.2.2",
		redpanda.WithEnableSASL(),
		redpanda.WithEnableKafkaAuthorization(),
		redpanda.WithNewServiceAccount("siesta", "secret"),
		redpanda.WithSuperusers("siesta"),
		redpanda.WithTLS(certPEM, keyPEM),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Terminate(context.Background()); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})
	broker, err := c.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	// The exact configuration a user would put in values: SASL_SSL, SCRAM, a private CA.
	// Credentials as files, the way the chart mounts the Secret.
	userFile, passFile := filepath.Join(t.TempDir(), "username"), filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(userFile, []byte("siesta"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(passFile, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := KafkaConfig{
		BootstrapServers: []string{broker},
		SecurityProtocol: "SASL_SSL",
		SASLMechanism:    "SCRAM-SHA-256",
		SASLUsernameFile: userFile,
		SASLPasswordFile: passFile,
		TLSCAFile:        caFile,
	}
	opts, err := cfg.Opts()
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, "secure"); err != nil {
		t.Fatal(err)
	}

	p, err := NewKafka(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	before, ok := p.Observe(ctx, []string{"secure"})
	if !ok || before["secure"] != "0" {
		t.Fatalf("authenticated probe must see the fresh topic, got %v ok=%v", before, ok)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "secure", Partition: 0, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if after, ok := p.Observe(ctx, []string{"secure"}); !ok || after["secure"] != "1" {
		t.Fatalf("want secure at 1, got %v ok=%v", after, ok)
	}

	// Wrong password must be unknown, never zero.
	bad := cfg
	bad.SASLUsernameFile, bad.SASLPasswordFile = "", ""
	bad.SASLUsername, bad.SASLPassword = "siesta", "wrong"
	badOpts, err := bad.Opts()
	if err != nil {
		t.Fatal(err)
	}
	bp, err := NewKafka(badOpts...)
	if err != nil {
		t.Fatal(err)
	}
	defer bp.Close()
	if _, ok := bp.Observe(ctx, []string{"secure"}); ok {
		t.Fatal("a failed authentication must report unknown")
	}
}

// serverCert makes a self-signed certificate valid for localhost, usable as both server cert and CA.
func serverCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "localhost"},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}
