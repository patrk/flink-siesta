// Package main wires the pieces together. Nothing in here makes a decision.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/patrk/flink-siesta/internal/controller"
	"github.com/patrk/flink-siesta/internal/decide"
	"github.com/patrk/flink-siesta/internal/flink"
	"github.com/patrk/flink-siesta/internal/metricsauth"
	"github.com/patrk/flink-siesta/internal/probe"
	"github.com/patrk/flink-siesta/internal/store"
)

// version is set at build time: -ldflags "-X main.version=v0.1.0".
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}
	if err := run(); err != nil {
		ctrl.Log.WithName("setup").Error(err, "exiting")
		os.Exit(1)
	}
}

// config is everything the flags say. Kept as a struct so that the wiring below reads as a
// list of decisions, and so a test could build a controller the way main does.
type config struct {
	prefix, namespace, brokers, metricsAddr, probeAddr, unrecoverable string
	dryRun, leaderElect, flinkRest, metricsSecure                     bool
	flinkRestPort, maxRestarts, concurrency, apiBurst                 int
	apiQPS                                                            float64
	restartWindow, restartBackoff, pollInterval, probeTimeout         time.Duration
	stallAfter, failingAfter                                          time.Duration
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.prefix, "annotation-prefix", "siesta.flink.io", "annotation prefix for policy and state")
	flag.StringVar(&c.namespace, "namespace", os.Getenv("POD_NAMESPACE"), "namespace to watch (empty = all)")
	flag.BoolVar(&c.dryRun, "dry-run", false, "record decisions in annotations but never patch spec")
	flag.StringVar(&c.brokers, "kafka-bootstrap", "", "comma-separated Kafka bootstrap servers (overrides KAFKA_BOOTSTRAP_SERVERS)")
	flag.StringVar(&c.metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint")
	flag.BoolVar(&c.metricsSecure, "metrics-secure", false, "serve metrics over HTTPS, only to callers the Kubernetes API authenticates and authorizes for GET /metrics")
	flag.StringVar(&c.probeAddr, "health-probe-bind-address", ":8081", "health endpoint")
	flag.BoolVar(&c.leaderElect, "leader-elect", true, "enable leader election")
	flag.IntVar(&c.maxRestarts, "restart-max", 3, "restarts allowed per window")
	flag.DurationVar(&c.restartWindow, "restart-window", 30*time.Minute, "restart budget window")
	flag.DurationVar(&c.restartBackoff, "restart-backoff", time.Minute, "first restart backoff; doubles each time within the window")
	flag.IntVar(&c.concurrency, "max-concurrent-reconciles", 4, "deployments reconciled at once; raise it with the number of deployments (a tick mostly waits on Kafka and the job)")
	flag.Float64Var(&c.apiQPS, "kube-api-qps", 50, "sustained requests per second to the Kubernetes API; a quiet tick costs one, so keep it above deployments/60")
	flag.IntVar(&c.apiBurst, "kube-api-burst", 100, "burst of requests to the Kubernetes API above kube-api-qps")
	flag.DurationVar(&c.pollInterval, "poll-interval", time.Minute, "how often each deployment is revisited")
	flag.DurationVar(&c.probeTimeout, "probe-timeout", 10*time.Second, "bound for the probes of one tick: the Kafka reads (offsets, lag) and each call to the job's REST API get this long each")
	flag.DurationVar(&c.stallAfter, "resume-stall-after", 10*time.Minute, "warn once if a resumed job is not RUNNING after this")
	flag.DurationVar(&c.failingAfter, "failing-after", 10*time.Minute, "RESTARTING longer than this counts as failing")
	flag.BoolVar(&c.flinkRest, "flink-rest", true, "ask the running job's REST API to verify sources and, with idle: job, whether it objects to sleeping")
	flag.IntVar(&c.flinkRestPort, "flink-rest-port", 8081, "port of the operator's <deployment>-rest Service")
	flag.StringVar(&c.unrecoverable, "unrecoverable-patterns", "UnknownTopicOrPartition,does not exist,ImagePullBackOff,ErrImagePull",
		"comma-separated substrings of status.reconciliationStatus.error or status.error that mean: never restart, on a job that is not running")
	flag.Parse()
	return c
}

// managerOptions are the manager's settings that follow from the flags.
func managerOptions(cfg config, scheme *runtime.Scheme) ctrl.Options {
	opts := ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: cfg.probeAddr,
		LeaderElection:         cfg.leaderElect,
		LeaderElectionID:       "siesta.flink.io",
		// FlinkDeployments are read as unstructured objects, which controller-runtime's client
		// fetches from the API server on every read unless told to use the cache it already
		// keeps for the watch. Without this every tick cost a GET, and the client's rate limit
		// capped a pass at 20 deployments a second.
		Client: client.Options{Cache: &client.CacheOptions{Unstructured: true}},
	}
	if cfg.namespace != "" {
		opts.Cache = cache.Options{DefaultNamespaces: map[string]cache.Config{cfg.namespace: {}}}
	}
	return opts
}

// run holds every defer; main only translates its error into an exit code.
func run() error {
	cfg := parseFlags()
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
	log := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme) // core types for Events and Leases; FlinkDeployment is handled as unstructured

	restCfg := ctrl.GetConfigOrDie()
	restCfg.QPS, restCfg.Burst = float32(cfg.apiQPS), cfg.apiBurst
	metricsOpts := metricsserver.Options{BindAddress: cfg.metricsAddr}
	if cfg.metricsSecure {
		// A self-signed certificate, and a TokenReview and SubjectAccessReview per scrape: the
		// caller's ServiceAccount needs get on the /metrics non-resource URL.
		metricsOpts.SecureServing = true
		metricsOpts.FilterProvider = metricsauth.Filter
	}
	opts := managerOptions(cfg, scheme)
	opts.Metrics = metricsOpts
	mgr, err := ctrl.NewManager(restCfg, opts)
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	kafkaCfg := probe.KafkaConfigFromEnv()
	if cfg.brokers != "" {
		kafkaCfg.BootstrapServers = strings.Split(cfg.brokers, ",")
	}
	kafkaOpts, err := kafkaCfg.Opts()
	if err != nil {
		return err
	}
	kafkaProbe, err := probe.NewKafka(kafkaOpts...)
	if err != nil {
		return fmt.Errorf("kafka probe: %w", err)
	}
	defer kafkaProbe.Close()

	r := &controller.Reconciler{
		Client:           mgr.GetClient(),
		Prefix:           cfg.prefix,
		DryRun:           cfg.dryRun,
		Probe:            kafkaProbe,
		Lag:              kafkaProbe,
		Gaps:             kafkaProbe,
		Recorder:         mgr.GetEventRecorder("siesta"),
		Store:            store.Store{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Prefix: cfg.prefix, Seen: &store.Seen{}},
		Now:              time.Now,
		PollInterval:     cfg.pollInterval,
		ProbeTimeout:     cfg.probeTimeout,
		ResumeStallAfter: cfg.stallAfter,
		Concurrency:      cfg.concurrency,
		Decider: decide.New(decide.RestartPolicy{
			MaxRestarts:   cfg.maxRestarts,
			Window:        cfg.restartWindow,
			BaseBackoff:   cfg.restartBackoff,
			Multiplier:    2,
			FailingAfter:  cfg.failingAfter,
			Unrecoverable: strings.Split(cfg.unrecoverable, ","),
		}),
	}
	r.Decider.Quiet = cfg.pollInterval
	if cfg.flinkRest {
		r.Flink = flink.NewREST(cfg.flinkRestPort)
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	_ = mgr.AddHealthzCheck("healthz", healthz.Ping)
	_ = mgr.AddReadyzCheck("readyz", healthz.Ping)

	log.Info("starting", "prefix", cfg.prefix, "namespace", cfg.namespace, "dryRun", cfg.dryRun, "concurrency", cfg.concurrency, "metricsSecure", cfg.metricsSecure)
	return mgr.Start(ctrl.SetupSignalHandler())
}
