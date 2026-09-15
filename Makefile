FLINK_OPERATOR_VERSION ?= 1.15.0
FLINK_VERSION ?= 2.2
ENVTEST_K8S_VERSION ?= 1.34.0
SOAK_TIMEOUT ?= 30m
IMAGE ?= siesta:e2e
KUBE_CONTEXT ?= kind-siesta
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: deps build test it envtest bench soak lint image kind-up kind-deps kind-down e2e e2e-job e2e-chaos e2e-soak

deps:
	go get sigs.k8s.io/controller-runtime@latest k8s.io/apimachinery@latest k8s.io/client-go@latest \
	       github.com/twmb/franz-go@latest github.com/twmb/franz-go/pkg/kadm@latest \
	       github.com/testcontainers/testcontainers-go/modules/kafka@latest github.com/testcontainers/testcontainers-go/modules/redpanda@latest
	go mod tidy

build: ; CGO_ENABLED=0 go build -o bin/siesta ./cmd
test:  ; go test ./...
it:    ; go test -count=1 -tags integration ./internal/probe/
lint:  ; golangci-lint run ./...

envtest:
	mkdir -p test/crds
	curl -sSL -o test/crds/flinkdeployments.yml \
	  https://raw.githubusercontent.com/apache/flink-kubernetes-operator/release-$(FLINK_OPERATOR_VERSION)/helm/flink-kubernetes-operator/crds/flinkdeployments.flink.apache.org-v1.yml
	go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use $(ENVTEST_K8S_VERSION) -p path > .envtest-path
	KUBEBUILDER_ASSETS=$$(cat .envtest-path) go test ./internal/controller/

soak:
	KUBEBUILDER_ASSETS=$$(cat .envtest-path) go test -tags soak -run TestSoak -v -timeout $(SOAK_TIMEOUT) ./internal/controller/

bench:
	KUBEBUILDER_ASSETS=$$(cat .envtest-path) go test -run '^$$' -bench Reconcile -benchmem -benchtime 300x ./internal/controller/

image: ; docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

kind-up:
	kind create cluster --name siesta --config e2e/kind.yaml
	$(MAKE) kind-deps

kind-deps:
	helm repo add --force-update flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-$(FLINK_OPERATOR_VERSION)/ && helm repo update
	helm --kube-context $(KUBE_CONTEXT) install flink-kubernetes-operator flink-operator/flink-kubernetes-operator --set webhook.create=false
	kubectl --context $(KUBE_CONTEXT) wait --for=condition=Available deploy/flink-kubernetes-operator --timeout=180s

# The e2e job: a Kafka source to a discarding sink, built on the Flink image under test.
e2e-job:
	bash e2e/job/build.sh $(FLINK_VERSION)
	kind load docker-image siesta-e2e-job:$(FLINK_VERSION) --name siesta

# All scenarios in their own namespaces, E2E_PARALLEL at a time (1 = in series, more on a big
# machine; a scenario needs about 3 GB), or one in the foreground: make e2e E2E_SCENARIO=job-graph
E2E_PARALLEL ?= 2
e2e: image e2e-job
	kind load docker-image $(IMAGE) --name siesta
	IMAGE_REPO=siesta IMAGE_TAG=e2e KUBE_CONTEXT=$(KUBE_CONTEXT) FLINK_VERSION=$(FLINK_VERSION) E2E_SCENARIO="$(E2E_SCENARIO)" E2E_PARALLEL=$(E2E_PARALLEL) bash e2e/run.sh

kind-down: ; kind delete cluster --name siesta

e2e-chaos: image e2e-job
	kind load docker-image $(IMAGE) --name siesta
	IMAGE_REPO=siesta IMAGE_TAG=e2e KUBE_CONTEXT=$(KUBE_CONTEXT) FLINK_VERSION=$(FLINK_VERSION) bash e2e/chaos.sh

e2e-soak: image e2e-job
	kind load docker-image $(IMAGE) --name siesta
	IMAGE_REPO=siesta IMAGE_TAG=e2e KUBE_CONTEXT=$(KUBE_CONTEXT) FLINK_VERSION=$(FLINK_VERSION) bash e2e/soak.sh
