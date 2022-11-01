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

## Source

The ClickHouse Source allows you to move data from the ClickHouse table to Conduit Destination connectors.

The iterator selects existing rows from the selected table in batches with ordering and where claus:
```
SELECT {{config.columns}}
FROM {{config.table}}
WHERE {{config.orderingColumn}} > {{position}}
ORDER BY {{config.orderingColumn}};
```

### CDC

When all existing rows have been read, the iterator will start selecting all newly inserted rows. CDC does not support
the capture of updated or deleted operation. All records have an `OperationCreate` operation.

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
| `keyColumns`     | Comma-separated list of column names to build the `sdk.Record.Key`. Column names are the keys of the `record.Key` map, and the values are taken from the row.       | **true** | `id,name`                                      |
| `orderingColumn` | Column name that the connector will use for ordering rows. Column must contain unique values and suitable for sorting, otherwise the snapshot won't work correctly. | **true** | `id`                                           |
| `columns`        | Comma-separated list of column names that should be included in each payload of the `sdk.Record`. By default includes all columns.                                  | false    | `id,name,age`                                  |
| `batchSize`      | Size of rows batch. Min is 1 and max is 100000. The default is 1000.                                                                                                | false    | `100`                                          |

## Destination

The ClickHouse Destination allows you to move data from any Conduit Source to a ClickHouse table. It takes
a `sdk.Record` and parses it into a valid SQL query.

### Key Handling

If the `sdk.Record.Key` is empty, it is formed from `sdk.Record.Payload` data by the keys of the `keyColumns` list (for
update operations only).

### Table Name

If a record contains a `clickhouse.table` property in its metadata, it will work with this table, otherwise, it will
fall back to use the table configured in the connector. Thus, a Destination can support multiple tables in a single
connector, as long as the user has proper access to those tables.

### Configuration Options

| name                 | description                                                                        | required | example                                        |
|----------------------|------------------------------------------------------------------------------------|----------|------------------------------------------------|
| `url`                | [DSN](https://github.com/ClickHouse/clickhouse-go#dsn) to connect to the database. | **true** | `http://username:password@host1:8123/database` |
| `table`              | Name of the table that the connector should write to.                              | **true** | `table_name`                                   |
| `keyColumns`         | Comma-separated list of keys for [Key Handling](#key-handling).                    | false    | `id,name`                                      |
| `sdk.rate.perSecond` | Maximum times the Write function can be called per second (0 means no rate limit). | false    | `200`                                          |
| `sdk.rate.burst`     | Allow bursts of at most X writes (0 means that bursts are not allowed).            | false    | `10`                                           |

## Known limitations

Creating a Source or Destination connector will fail if the table does not exist or if the user does not have permission
to work with the specified table.
