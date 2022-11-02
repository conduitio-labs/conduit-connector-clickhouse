# Conduit Connector ClickHouse

## General

ClickHouse connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides a destination
ClickHouse connector.

Connector uses [Golang SQL database driver](https://github.com/ClickHouse/clickhouse-go) for Yandex ClickHouse.

## Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.49.0

## How to build it

Run `make build`.

## Testing

Run `make test` to run all unit and integration tests. To run the integration test, set the ClickHouse database URL to
the environment variables as an `CLICKHOUSE_URL`.

## Destination

The ClickHouse Destination allows you to move data from any Conduit Source to a ClickHouse table. It takes
a `sdk.Record` and parses it into a valid SQL query.

### Table Name

If a record contains a `clickhouse.table` property in its metadata, it will work with this table, otherwise, it will
fall back to use the table configured in the connector. Thus, a Destination can support multiple tables in a single
connector, as long as the user has proper access to those tables.

### Configuration Options

| name                 | description                                                                        | required | example                                        |
|----------------------|------------------------------------------------------------------------------------|----------|------------------------------------------------|
| `url`                | [DSN](https://github.com/ClickHouse/clickhouse-go#dsn) to connect to the database. | **true** | `http://username:password@host1:8123/database` |
| `table`              | Name of the table that the connector should write to.                              | **true** | `table_name`                                   |
| `keyColumns`         | Comma-separated list of column names for [key handling](#key-handling).            | false    | `id,name`                                      |
| `sdk.rate.perSecond` | Maximum times the Write function can be called per second (0 means no rate limit). | false    | `200`                                          |
| `sdk.rate.burst`     | Allow bursts of at most X writes (0 means that bursts are not allowed).            | false    | `10`                                           |

#### Key Handling

If the `sdk.Record.Key` is empty, it is formed from `sdk.Record.Payload` data by the comma-separated `keyColumns` list
of keys (for update operations only).

Creating a Destination connector will fail if the table does not exist or if the user does not have permission to work
with the specified table.
