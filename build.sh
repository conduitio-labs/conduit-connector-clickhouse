make
mv conduit-connector-clickhouse /Users/bernard/Documents/work/projects/inreality/workspace_py/cdc/ir-cdc-conduit/tests/test_01/connectors


GOOS=linux \
GOARCH=arm64 \
CGO_ENABLED=0 \
go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-clickhouse.version=43a8d4a-dirty'" -o conduit-connector-clickhouse cmd/connector/main.go