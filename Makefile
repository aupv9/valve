BUF ?= buf
GOLANGCI_LINT ?= golangci-lint

.PHONY: all build test bench lint proto validate run-local clean

all: proto lint test build

build:
	go build -o bin/ ./cmd/...

test:
	go test -race ./...

bench:
	go test -run '^$$' -bench . -benchmem ./internal/...

lint:
	go vet ./...
	$(GOLANGCI_LINT) run ./...
	$(BUF) lint

proto:
	$(BUF) generate

validate:
	go run ./cmd/configurator validate policies/*.yaml

# Full loop on one machine: controller <- aggregator <- agent with synthetic
# load at 1.5x the first bucket's limit (expect ~33% rejected). Ctrl-C to stop.
run-local: build
	@trap 'kill 0' INT TERM EXIT; \
	./bin/controller -listen 127.0.0.1:9091 & \
	sleep 0.5; ./bin/aggregator -listen 127.0.0.1:9090 -controller 127.0.0.1:9091 & \
	sleep 0.5; ./bin/agent -aggregator 127.0.0.1:9090 -load-rps 30000

clean:
	rm -rf bin
