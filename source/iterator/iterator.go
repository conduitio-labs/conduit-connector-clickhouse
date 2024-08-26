// Copyright © 2022 Meroxa, Inc. & Yalantis
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iterator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/conduitio-labs/conduit-connector-clickhouse/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
	"go.uber.org/multierr"
)

// metadataFieldTable is a name of a record metadata field that stores a ClickHouse table name.
const metadataFieldTable = "clickhouse.table"

// Iterator is an implementation of an iterator for ClickHouse.
type Iterator struct {
	db       *sqlx.DB
	rows     *sqlx.Rows
	position *Position

	// table name
	table string
	// name of the column that iterator will use for setting key in record
	keyColumns []string
	// name of the column that iterator will use for sorting data
	orderingColumn string
	// size of batch
	batchSize int
}

// New creates a new instance of the iterator.
func New(ctx context.Context, driverName string, pos *Position, config config.SourceConfig) (*Iterator, error) {
	var err error

	iterator := &Iterator{
		position:       pos,
		table:          config.Table,
		keyColumns:     config.KeyColumns,
		orderingColumn: config.OrderingColumn,
		batchSize:      config.BatchSize,
	}

	iterator.db, err = sqlx.Open(driverName, config.URL)
	if err != nil {
		return nil, fmt.Errorf("open db connection: %w", err)
	}

	if iterator.position.LastProcessedValue == nil {
		latestSnapshotValue, latestValErr := iterator.latestSnapshotValue(ctx)
		if latestValErr != nil {
			return nil, fmt.Errorf("get latest snapshot value: %w", latestValErr)
		}

		if config.Snapshot {
			// set the LatestSnapshotValue to specify which record the snapshot iterator will work to
			iterator.position.LatestSnapshotValue = latestSnapshotValue
		} else {
			// set the LastProcessedValue to skip a snapshot of the entire table
			iterator.position.LastProcessedValue = latestSnapshotValue
		}
	}

	options, err := clickhouse.ParseDSN(config.URL)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	err = iterator.populateKeyColumns(ctx, options.Auth.Database)
	if err != nil {
		return nil, fmt.Errorf("populate key columns: %w", err)
	}

	err = iterator.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return iterator, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (iter *Iterator) HasNext(ctx context.Context) (bool, error) {
	if iter.rows != nil && iter.rows.Next() {
		return true, nil
	}

	if err := iter.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	if iter.rows.Next() {
		return true, nil
	}

	if iter.position.LatestSnapshotValue != nil {
		// switch to CDC mode
		iter.position.LastProcessedValue = iter.position.LatestSnapshotValue
		iter.position.LatestSnapshotValue = nil

		// and load new rows
		if err := iter.loadRows(ctx); err != nil {
			return false, fmt.Errorf("load rows: %w", err)
		}

		return iter.rows.Next(), nil
	}

	return false, nil
}

// Next returns the next record.
func (iter *Iterator) Next(context.Context) (opencdc.Record, error) {
	row := make(map[string]any)
	if err := iter.rows.MapScan(row); err != nil {
		return opencdc.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	if _, ok := row[iter.orderingColumn]; !ok {
		return opencdc.Record{}, fmt.Errorf("ordering column %q not found", iter.orderingColumn)
	}

	key := make(opencdc.StructuredData)
	for i := range iter.keyColumns {
		val, ok := row[iter.keyColumns[i]]
		if !ok {
			return opencdc.Record{}, fmt.Errorf("key column %q not found", iter.keyColumns[i])
		}

		key[iter.keyColumns[i]] = val
	}

	rowBytes, err := json.Marshal(row)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	// set a new position into the variable,
	// to avoid saving position into the struct until we marshal the position
	position := *iter.position
	// set the value from iter.orderingColumn column you chose
	position.LastProcessedValue = row[iter.orderingColumn]

	convertedPosition, err := position.marshal()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("convert position :%w", err)
	}

	iter.position = &position

	metadata := opencdc.Metadata{
		metadataFieldTable: iter.table,
	}
	metadata.SetCreatedAt(time.Now())

	if position.LatestSnapshotValue != nil {
		return sdk.Util.Source.NewRecordSnapshot(convertedPosition, metadata, key, opencdc.RawData(rowBytes)), nil
	}

	return sdk.Util.Source.NewRecordCreate(convertedPosition, metadata, key, opencdc.RawData(rowBytes)), nil
}

// Stop stops iterators and closes database connection.
func (iter *Iterator) Stop() error {
	var err error

	if iter.rows != nil {
		err = iter.rows.Close()
	}

	if iter.db != nil {
		err = multierr.Append(err, iter.db.Close())
	}

	if err != nil {
		return fmt.Errorf("close db rows and db: %w", err)
	}

	return nil
}

// loadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (iter *Iterator) loadRows(ctx context.Context) error {
	sb := sqlbuilder.NewSelectBuilder().
		Select("*").
		From(iter.table).
		OrderBy(iter.orderingColumn).
		Limit(iter.batchSize)

	if iter.position.LastProcessedValue != nil {
		sb.Where(sb.GreaterThan(iter.orderingColumn, iter.position.LastProcessedValue))
	}

	if iter.position.LatestSnapshotValue != nil {
		sb.Where(sb.LessEqualThan(iter.orderingColumn, iter.position.LatestSnapshotValue))
	}

	query, args := sb.Build()

	rows, err := iter.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select query %q, %v: %w", query, args, err)
	}

	iter.rows = rows

	return nil
}

// populateKeyColumns populates keyColumn from the database metadata
// or from the orderingColumn configuration field in the described order if it's empty.
func (iter *Iterator) populateKeyColumns(ctx context.Context, database string) error {
	if len(iter.keyColumns) != 0 {
		return nil
	}

	sb := sqlbuilder.NewSelectBuilder().
		Select("name").
		From("system.columns").
		Where("is_in_primary_key = 1")

	sb.Where(sb.Equal("table", iter.table))

	if database != "" {
		sb.Where(sb.Equal("database", database))
	}

	query, args := sb.Build()

	//nolint:sqlclosecheck // false positive, see: https://github.com/ryanrolds/sqlclosecheck/issues/35
	rows, err := iter.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select primary keys query %q, %v: %w", query, args, err)
	}
	defer rows.Close()

	keyColumn := ""
	for rows.Next() {
		if err = rows.Scan(&keyColumn); err != nil {
			return fmt.Errorf("scan key column value: %w", err)
		}

		iter.keyColumns = append(iter.keyColumns, keyColumn)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate over rows: %w", err)
	}

	if len(iter.keyColumns) != 0 {
		return nil
	}

	iter.keyColumns = []string{iter.orderingColumn}

	return nil
}

// latestSnapshotValue returns most recent value of orderingColumn column.
func (iter *Iterator) latestSnapshotValue(ctx context.Context) (any, error) {
	var latestSnapshotValue any

	query := sqlbuilder.NewSelectBuilder().
		Select(iter.orderingColumn).
		From(iter.table).
		OrderBy(iter.orderingColumn).Desc().
		Limit(1).
		String()

	//nolint:sqlclosecheck // false positive, see: https://github.com/ryanrolds/sqlclosecheck/issues/35
	rows, err := iter.db.QueryxContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("execute select latest snapshot value query %q: %w", query, err)
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&latestSnapshotValue); err != nil {
			return nil, fmt.Errorf("scan latest snapshot value: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over rows: %w", err)
	}

	return latestSnapshotValue, nil
}
