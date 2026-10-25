package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

// The deployment is read on every tick. It must come from the watch's cache: a live GET per
// tick halves what the API rate limit lets one controller visit (the 2,000 deployment load test).
func TestFlinkDeploymentsAreReadFromTheCache(t *testing.T) {
	opts := managerOptions(config{namespace: "flink"}, runtime.NewScheme())
	if opts.Client.Cache == nil || !opts.Client.Cache.Unstructured {
		t.Fatal("unstructured reads must be served from the cache")
	}
	if _, ok := opts.Cache.DefaultNamespaces["flink"]; !ok {
		t.Fatal("the cache must be limited to the watched namespace")
	}
}
