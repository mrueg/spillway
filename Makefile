SHELL := /usr/bin/env bash

BINARY  ?= spillway
BIN_DIR ?= bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

LDFLAGS := -X github.com/mrueg/spillway/pkg/version.Version=$(VERSION)

.PHONY: all
all: verify build

.PHONY: build
build:
	go build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BINARY) ./cmd/spillway

.PHONY: test
test:
	go test ./...

# Every build goes through goreleaser, which drives ko for the image side.
# There is no Dockerfile and no separate ko config.
GORELEASER ?= goreleaser
GOVULNCHECK_VERSION ?= latest

# Signing and SBOMs are release-time concerns: they need cosign and syft, and
# keyless signing blocks on an interactive Sigstore flow outside CI. goreleaser
# runs both pipes for snapshots too, so local builds skip them explicitly.
LOCAL_SKIP := publish,announce,sbom,sign

# A local image, loaded into the docker daemon as goreleaser.ko.local:<version>.
.PHONY: image
image:
	$(GORELEASER) release --snapshot --clean --skip=$(LOCAL_SKIP),archive

# The full snapshot: binaries, archives, checksums, and the image.
.PHONY: snapshot
snapshot:
	$(GORELEASER) release --snapshot --clean --skip=$(LOCAL_SKIP)

# The e2e suite needs a provisioned environment (kind + kcp); see hack/e2e.sh.
.PHONY: e2e
e2e:
	./hack/e2e.sh all

.PHONY: e2e-up
e2e-up:
	./hack/e2e.sh up

.PHONY: e2e-test
e2e-test:
	./hack/e2e.sh test

.PHONY: e2e-down
e2e-down:
	./hack/e2e.sh down

# Compares a native CRD against the same resource offloaded to kcp. Needs a
# provisioned environment: make e2e-up first.
.PHONY: bench
bench:
	go test -tags bench -count=1 -timeout 90m -v ./test/bench/...

.PHONY: fmt
fmt:
	go fmt ./...

# CI cannot rewrite the tree, so it checks instead of formatting.
.PHONY: fmt-check
fmt-check:
	@unformatted=$$(gofmt -l . ); \
	if [ -n "$$unformatted" ]; then \
		echo "these files need gofmt:"; echo "$$unformatted"; exit 1; \
	fi

# Fails if go.mod or go.sum would change, without leaving the change behind.
.PHONY: tidy-check
tidy-check:
	@cp go.mod go.mod.orig && cp go.sum go.sum.orig
	@go mod tidy
	@status=0; \
	if ! cmp -s go.mod go.mod.orig || ! cmp -s go.sum go.sum.orig; then \
		echo "go mod tidy produced changes; run 'make tidy' and commit the result"; status=1; \
	fi; \
	mv go.mod.orig go.mod; mv go.sum.orig go.sum; exit $$status

# -race is not decoration here: the resource cache is written by a refresh loop
# and read by every discovery request.
.PHONY: cover
cover:
	go test -race -covermode=atomic -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint:
	golangci-lint run

# govulncheck is installed rather than "go run": go run exits 1 for any non-zero
# program exit, which would hide the 3 that distinguishes findings from errors.
#
# Fails only on vulnerabilities this code can actually reach. govulncheck exits
# 3 for any finding at all, including ones in required-but-uncalled modules,
# which would red-CI for something no change here can fix. A tool failure still
# fails the target.
.PHONY: vulncheck
vulncheck:
	@set +e; \
	bin=$$(mktemp -d); \
	GOBIN=$$bin go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) \
	  || { echo "==> could not install govulncheck"; rm -rf $$bin; exit 1; }; \
	output=$$($$bin/govulncheck ./... 2>&1); \
	code=$$?; \
	rm -rf $$bin; \
	echo "$$output"; \
	case $$code in \
	  0) exit 0 ;; \
	  3) if echo "$$output" | grep -q "Your code is affected by"; then \
	       echo "==> reachable vulnerabilities found"; exit 1; \
	     fi; \
	     echo "==> only unreachable vulnerabilities in required modules; not failing"; \
	     exit 0 ;; \
	  *) echo "==> govulncheck failed to run"; exit $$code ;; \
	esac

# Both tag sets: the e2e suite is behind a build tag and would otherwise never
# be vetted.
.PHONY: vet
vet:
	go vet ./...
	go vet -tags e2e ./...

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: verify
verify: fmt vet test

.PHONY: clean
clean:
	rm -rf $(BIN_DIR)
