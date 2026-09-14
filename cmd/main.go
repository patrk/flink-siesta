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
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/patrk/flink-siesta/internal/controller"
	"github.com/patrk/flink-siesta/internal/decide"
	"github.com/patrk/flink-siesta/internal/flink"
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
	dryRun, leaderElect, flinkRest                                    bool
	flinkRestPort, maxRestarts                                        int
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
	flag.StringVar(&c.probeAddr, "health-probe-bind-address", ":8081", "health endpoint")
	flag.BoolVar(&c.leaderElect, "leader-elect", true, "enable leader election")
	flag.IntVar(&c.maxRestarts, "restart-max", 3, "restarts allowed per window")
	flag.DurationVar(&c.restartWindow, "restart-window", 30*time.Minute, "restart budget window")
	flag.DurationVar(&c.restartBackoff, "restart-backoff", time.Minute, "first restart backoff; doubles each time within the window")
	flag.DurationVar(&c.pollInterval, "poll-interval", time.Minute, "how often each deployment is revisited")
	flag.DurationVar(&c.probeTimeout, "probe-timeout", 10*time.Second, "bound for the probes of one tick: offsets, lag and the job's REST API together")
	flag.DurationVar(&c.stallAfter, "resume-stall-after", 10*time.Minute, "warn once if a resumed job is not RUNNING after this")
	flag.DurationVar(&c.failingAfter, "failing-after", 10*time.Minute, "RESTARTING longer than this counts as failing")
	flag.BoolVar(&c.flinkRest, "flink-rest", true, "ask the running job's REST API to verify sources and, with idle: job, whether it objects to sleeping")
	flag.IntVar(&c.flinkRestPort, "flink-rest-port", 8081, "port of the operator's <deployment>-rest Service")
	flag.StringVar(&c.unrecoverable, "unrecoverable-patterns", "UnknownTopicOrPartition,does not exist,ImagePullBackOff,ErrImagePull",
		"comma-separated substrings of status.reconciliationStatus.error that mean: never restart")
	flag.Parse()
	return c
}

// run holds every defer; main only translates its error into an exit code.
func run() error {
	cfg := parseFlags()
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
	log := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme) // core types for Events and Leases; FlinkDeployment is handled as unstructured

	opts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: cfg.metricsAddr},
		HealthProbeBindAddress: cfg.probeAddr,
		LeaderElection:         cfg.leaderElect,
		LeaderElectionID:       "siesta.flink.io",
	}
	if cfg.namespace != "" {
		opts.Cache = cache.Options{DefaultNamespaces: map[string]cache.Config{cfg.namespace: {}}}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), opts)
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
		Recorder:         mgr.GetEventRecorder("siesta"),
		Store:            store.Store{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Prefix: cfg.prefix},
		Now:              time.Now,
		PollInterval:     cfg.pollInterval,
		ProbeTimeout:     cfg.probeTimeout,
		ResumeStallAfter: cfg.stallAfter,
		Decider: decide.New(decide.RestartPolicy{
			MaxRestarts:   cfg.maxRestarts,
			Window:        cfg.restartWindow,
			BaseBackoff:   cfg.restartBackoff,
			Multiplier:    2,
			FailingAfter:  cfg.failingAfter,
			Unrecoverable: strings.Split(cfg.unrecoverable, ","),
		}),
	}
	if cfg.flinkRest {
		r.Flink = flink.NewREST(cfg.flinkRestPort)
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	_ = mgr.AddHealthzCheck("healthz", healthz.Ping)
	_ = mgr.AddReadyzCheck("readyz", healthz.Ping)

	log.Info("starting", "prefix", cfg.prefix, "namespace", cfg.namespace, "dryRun", cfg.dryRun)
	return mgr.Start(ctrl.SetupSignalHandler())
}
