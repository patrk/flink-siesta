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

// run holds every defer; main only translates its error into an exit code.
func run() error {
	var (
		prefix         = flag.String("annotation-prefix", "siesta.flink.io", "annotation prefix for policy and state")
		namespace      = flag.String("namespace", os.Getenv("POD_NAMESPACE"), "namespace to watch (empty = all)")
		dryRun         = flag.Bool("dry-run", false, "record decisions in annotations but never patch spec")
		brokers        = flag.String("kafka-bootstrap", "", "comma-separated Kafka bootstrap servers (overrides KAFKA_BOOTSTRAP_SERVERS)")
		metricsAddr    = flag.String("metrics-bind-address", ":8080", "metrics endpoint")
		probeAddr      = flag.String("health-probe-bind-address", ":8081", "health endpoint")
		leaderElect    = flag.Bool("leader-elect", true, "enable leader election")
		maxRestarts    = flag.Int("restart-max", 3, "restarts allowed per window")
		restartWindow  = flag.Duration("restart-window", 30*time.Minute, "restart budget window")
		restartBackoff = flag.Duration("restart-backoff", time.Minute, "first restart backoff; doubles each time within the window")
		failingAfter   = flag.Duration("failing-after", 10*time.Minute, "RESTARTING longer than this counts as failing")
		unrecoverable  = flag.String("unrecoverable-patterns", "UnknownTopicOrPartition,does not exist,ImagePullBackOff,ErrImagePull",
			"comma-separated substrings of status.reconciliationStatus.error that mean: never restart")
	)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
	log := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme) // core types for Events and Leases; FlinkDeployment is handled as unstructured

	opts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: *metricsAddr},
		HealthProbeBindAddress: *probeAddr,
		LeaderElection:         *leaderElect,
		LeaderElectionID:       "siesta.flink.io",
	}
	if *namespace != "" {
		opts.Cache = cache.Options{DefaultNamespaces: map[string]cache.Config{*namespace: {}}}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), opts)
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	kafkaCfg := probe.KafkaConfigFromEnv()
	if *brokers != "" {
		kafkaCfg.BootstrapServers = strings.Split(*brokers, ",")
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
		Client:   mgr.GetClient(),
		Prefix:   *prefix,
		DryRun:   *dryRun,
		Probe:    kafkaProbe,
		Lag:      kafkaProbe,
		Recorder: mgr.GetEventRecorder("siesta"),
		Store:    store.Store{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Prefix: *prefix},
		Now:      time.Now,
		Decider: decide.New(decide.RestartPolicy{
			MaxRestarts:   *maxRestarts,
			Window:        *restartWindow,
			BaseBackoff:   *restartBackoff,
			Multiplier:    2,
			FailingAfter:  *failingAfter,
			Unrecoverable: strings.Split(*unrecoverable, ","),
		}),
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	_ = mgr.AddHealthzCheck("healthz", healthz.Ping)
	_ = mgr.AddReadyzCheck("readyz", healthz.Ping)

	log.Info("starting", "prefix", *prefix, "namespace", *namespace, "dryRun", *dryRun)
	return mgr.Start(ctrl.SetupSignalHandler())
}
