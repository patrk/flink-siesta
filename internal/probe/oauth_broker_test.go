//go:build integration

package probe

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OAUTHBEARER against a real Apache Kafka broker. With no validator configured, Kafka accepts
// unsigned JWTs ("alg": "none") that carry a sub claim, which is enough to prove the token
// travels the way a broker expects; signature checks are the identity provider's business.
func TestKafkaProbeWithOAuthBearer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := testcontainers.NewDockerProvider(); err != nil {
		t.Skipf("docker not available: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	p := fmt.Sprintf("%d/tcp", port)
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apache/kafka:4.3.1",
			ExposedPorts: []string{p},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = network.PortMap{network.MustParsePort(p): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: fmt.Sprint(port)}}}
			},
			Env: map[string]string{
				"KAFKA_NODE_ID":                              "1",
				"KAFKA_PROCESS_ROLES":                        "broker,controller",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":             "1@localhost:9093",
				"KAFKA_LISTENERS":                            fmt.Sprintf("SASL_PLAINTEXT://:%d,CONTROLLER://:9093", port),
				"KAFKA_ADVERTISED_LISTENERS":                 fmt.Sprintf("SASL_PLAINTEXT://127.0.0.1:%d", port),
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":       "SASL_PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
				"KAFKA_CONTROLLER_LISTENER_NAMES":            "CONTROLLER",
				"KAFKA_INTER_BROKER_LISTENER_NAME":           "SASL_PLAINTEXT",
				"KAFKA_SASL_ENABLED_MECHANISMS":              "OAUTHBEARER",
				"KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL": "OAUTHBEARER",
				"KAFKA_OPTS":                                 "-Djava.security.auth.login.config=/etc/kafka/jaas.conf",
				"CLUSTER_ID":                                 "siesta-oauth-test-0",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":     "1",
			},
			Files: []testcontainers.ContainerFile{{
				ContainerFilePath: "/etc/kafka/jaas.conf", FileMode: 0o644,
				Reader: strings.NewReader(`KafkaServer {
  org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub="broker";
};
`),
			}},
			WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	unsigned := func(sub string) string {
		enc := base64.RawURLEncoding.EncodeToString
		now := time.Now().Unix()
		return enc([]byte(`{"alg":"none"}`)) + "." + enc([]byte(fmt.Sprintf(`{"sub":%q,"iat":%d,"exp":%d}`, sub, now, now+3600))) + "."
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(unsigned("siesta")), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := KafkaConfig{BootstrapServers: []string{fmt.Sprintf("127.0.0.1:%d", port)}, SecurityProtocol: "SASL_PLAINTEXT",
		SASLMechanism: "OAUTHBEARER", OAuthTokenFile: tokenFile}
	opts, err := cfg.Opts()
	if err != nil {
		t.Fatal(err)
	}
	admin, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopic(ctx, 1, 1, nil, "in"); err != nil {
		t.Fatalf("an authenticated client must be able to create a topic: %v", err)
	}
	probe, err := NewKafka(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if o, err := probe.Observe(ctx, []string{"in"}); err != nil || o.Snapshot["in"] != "0" {
		t.Fatalf("want offsets read over OAUTHBEARER, got %v %v", o, err)
	}

	// A token the broker rejects is an error, not an empty answer.
	if err := os.WriteFile(tokenFile, []byte("not-a-jwt"), 0o600); err != nil {
		t.Fatal(err)
	}
	bad, err := NewKafka(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	if _, err := bad.Observe(ctx, []string{"in"}); err == nil {
		t.Fatal("a rejected token must make the offsets unknown")
	}
}
