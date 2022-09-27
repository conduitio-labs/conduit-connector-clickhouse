.PHONY: build test

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-clickhouse.version=${VERSION}'" -o conduit-connector-clickhouse cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -count=1 -race ./...

lint:
	golangci-lint run -c .golangci.yml --go=1.18

dep:
	go mod download
	go mod tidy
