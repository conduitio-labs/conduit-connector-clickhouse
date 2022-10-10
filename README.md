# Conduit Connector ClickHouse

## General

ClickHouse connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides a destination
ClickHouse connector.

## Prerequisites

- [Go](https://go.dev/) 1.18
- [Golang SQL database client for ClickHouse](https://github.com/ClickHouse/clickhouse-go) 2.3.0
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.49.0
- (optional) [mock](https://github.com/golang/mock) 1.6.0

## How to build it

Run `make build`.

## Testing

Run `make test` to run all unit and integration tests. To pass the integration test, set the ClickHouse database URL to
the environment variables as an `CLICKHOUSE_URL`.

## Destination

The ClickHouse Destination takes a `sdk.Record` and parses it into a valid SQL query.

### Table name

If a record contains a `clickhouse.table` property in its metadata, it will work with this table, otherwise, it will
fall back to use the table configured in the connector. Thus, a Destination can support multiple tables in a single
connector, as long as the user has proper access to those tables.

### Configuration Options

| name        | description                                                                        | required | example                                                           |
|-------------|------------------------------------------------------------------------------------|----------|-------------------------------------------------------------------|
| `url`       | string line for connection to ClickHouse                                           | **true** | `localhost:8123?username=user&password=password&database=default` |
| `table`     | the name of a table in the database that the connector should write to, by default | **true** | `users`                                                           |
