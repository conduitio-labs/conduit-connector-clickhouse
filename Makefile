.PHONY: build test lint dep mockgen

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-clickhouse.version=${VERSION}'" -o conduit-connector-clickhouse cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -count=1 -race ./...

lint:
	golangci-lint run -c .golangci.yml

dep:
	go mod download
	go mod tidy

mockgen:
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go
	mockgen -package mock -source source/source.go -destination source/mock/source.go
