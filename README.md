# Conduit Connector ClickHouse

## General

ClickHouse connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a Source and a
Destination ClickHouse connectors.

Connector uses [Golang SQL database driver](https://github.com/ClickHouse/clickhouse-go) for Yandex ClickHouse.

## Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.49.0

## How to build it

Run `make build`.

## Testing

Run `make test` to run all unit and integration tests. To run the integration test, set the ClickHouse database URL to
the environment variables as an `CLICKHOUSE_URL`.

## Source

The ClickHouse Source Connector allows you to move data from the ClickHouse table to Conduit Destination connectors.

It supports all engines of the [MergeTree](https://clickhouse.com/docs/en/engines/table-engines/#mergetree)
and [Log](https://clickhouse.com/docs/en/engines/table-engines/#log) families.

The iterator selects existing rows from the selected table in batches with ordering and where claus:

```
SELECT {{config.columns}}
FROM {{config.table}}
WHERE {{config.orderingColumn}} > {{position}}
ORDER BY {{config.orderingColumn}};
```

### CDC

When all existing data has been read, the connector will only detect new rows.

### Position

The position contains the `orderingColumn` value of the last processed row. This value is used in the where clause of
the SELECT query.

### Table Name

The metadata of each record is appended by the `clickhouse.table` key with the value of the table name from the
configuration.

### Configuration Options

| name             | description                                                                                                                                                         | required | example                                        |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------------------------------------------------|
| `url`            | [DSN](https://github.com/ClickHouse/clickhouse-go#dsn) to connect to the database.                                                                                  | **true** | `http://username:password@host1:8123/database` |
| `table`          | Name of the table that the connector should read.                                                                                                                   | **true** | `table_name`                                   |
| `orderingColumn` | Column name that the connector will use for ordering rows. Column must contain unique values and suitable for sorting, otherwise the snapshot won't work correctly. | **true** | `id`                                           |
| `keyColumns`     | Comma-separated list of column names to build the `sdk.Record.Key`. See more: [key handling](#key-handling).                                                        | false    | `id,name`                                      |
| `columns`        | Comma-separated list of column names that should be included in each payload of the `sdk.Record`. By default includes all columns.                                  | false    | `id,name,age`                                  |
| `batchSize`      | Size of rows batch. Min is 1 and max is 100000. The default is 1000.                                                                                                | false    | `100`                                          |

#### Key handling

The `keyColumns` is an optional field. If the field is empty, the system makes a request to the database and uses the
received list of primary keys of the specified table. If the table does not contain primary keys, the system uses the
value of the `orderingColumn` field as the `keyColumns` value.

## Destination

The ClickHouse Destination allows you to move data from any Conduit Source to a ClickHouse table. It takes
a `sdk.Record` and parses it into a valid SQL
query. [Log family engines](https://clickhouse.com/docs/en/engines/table-engines/#log) do not support data changes, so
in case of `OperationUpdate` or `OperationDelete` operations they will return the next
error: `Table engine {{table_engine}} doesn't support mutations.`

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

## Known limitations

Creating a Source or Destination connector will fail if the table does not exist or if the user does not have permission
to work with the specified table.
