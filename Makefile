FLINK_OPERATOR_VERSION ?= 1.15.0
ENVTEST_K8S_VERSION ?= 1.34.0
IMAGE ?= siesta:e2e
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: deps build test it envtest lint image kind-up kind-deps kind-down e2e

deps:
	go get sigs.k8s.io/controller-runtime@latest k8s.io/apimachinery@latest k8s.io/client-go@latest \
	       github.com/twmb/franz-go@latest github.com/twmb/franz-go/pkg/kadm@latest \
	       github.com/testcontainers/testcontainers-go/modules/kafka@latest
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

image: ; docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

kind-up:
	kind create cluster --name siesta --config e2e/kind.yaml
	$(MAKE) kind-deps

kind-deps:
	kubectl create -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.2/cert-manager.yaml
	kubectl wait --for=condition=Available deploy -n cert-manager --all --timeout=180s
	helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-$(FLINK_OPERATOR_VERSION)/ && helm repo update
	helm install flink-kubernetes-operator flink-operator/flink-kubernetes-operator --set webhook.create=false
	kubectl wait --for=condition=Available deploy/flink-kubernetes-operator --timeout=180s

e2e: image
	kind load docker-image $(IMAGE) --name siesta
	IMAGE_REPO=siesta IMAGE_TAG=e2e bash e2e/run.sh

kind-down: ; kind delete cluster --name siesta
