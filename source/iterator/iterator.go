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
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

// metadata related.
const (
	metadataTable = "clickhouse.table"

	querySelectRowsFmt = "SELECT %s FROM %s%s ORDER BY %s ASC LIMIT %d;"
	whereClauseFmt     = " WHERE %s > ?"

	querySelectPKs = `
SELECT 
  name 
FROM 
  system.columns 
WHERE 
  is_in_primary_key = 1 
  AND table = ? 
  AND database = ?;
`
)

// Iterator is an implementation of an iterator for ClickHouse.
type Iterator struct {
	db   *sqlx.DB
	rows *sqlx.Rows

	// last processed orderingColumn value
	lastProcessedVal any
	// table name
	table string
	// name of the column that iterator will use for setting key in record
	keyColumns []string
	// name of the column that iterator will use for sorting data
	orderingColumn string
	// list of table's columns for record payload.
	// if empty - will get all columns
	columns []string
	// size of batch
	batchSize int
	// database name
	database string
}

// Params is an incoming iterator params for the New function.
type Params struct {
	DB               *sqlx.DB
	LastProcessedVal any
	Table            string
	KeyColumns       []string
	OrderingColumn   string
	Columns          []string
	BatchSize        int
	Database         string
}

// New creates a new instance of the iterator.
func New(ctx context.Context, params Params) (*Iterator, error) {
	iterator := &Iterator{
		db:               params.DB,
		lastProcessedVal: params.LastProcessedVal,
		table:            params.Table,
		keyColumns:       params.KeyColumns,
		orderingColumn:   params.OrderingColumn,
		columns:          params.Columns,
		batchSize:        params.BatchSize,
		database:         params.Database,
	}

	err := iterator.populateKeyColumns(ctx)
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
	return iter.hasNext(ctx)
}

// Next returns the next record.
func (iter *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := iter.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	if _, ok := row[iter.orderingColumn]; !ok {
		return sdk.Record{}, fmt.Errorf("ordering column %q not found", iter.orderingColumn)
	}

	key := make(sdk.StructuredData)
	for i := range iter.keyColumns {
		val, ok := row[iter.keyColumns[i]]
		if !ok {
			return sdk.Record{}, fmt.Errorf("key column %q not found", iter.keyColumns[i])
		}

		key[iter.keyColumns[i]] = val
	}

	rowBytes, err := json.Marshal(row)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	// set a new position into the variable,
	// to avoid saving position into the struct until we marshal the position
	positionBytes, err := json.Marshal(row[iter.orderingColumn])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	iter.lastProcessedVal = row[iter.orderingColumn]

	metadata := sdk.Metadata{
		metadataTable: iter.table,
	}
	metadata.SetCreatedAt(time.Now())

	return sdk.Util.Source.NewRecordCreate(
		positionBytes,
		metadata,
		key,
		sdk.RawData(rowBytes),
	), nil
}

// Stop stops iterators and closes database connection.
func (iter *Iterator) Stop() (err error) {
	if iter.rows != nil {
		return iter.rows.Close()
	}

	return nil
}

// returns a bool indicating whether the source has the next record to return or not.
func (iter *Iterator) hasNext(ctx context.Context) (bool, error) {
	if iter.rows != nil && iter.rows.Next() {
		return true, nil
	}

	if err := iter.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return iter.rows.Next(), nil
}

// selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (iter *Iterator) loadRows(ctx context.Context) error {
	columns := "*"
	if len(iter.columns) > 0 {
		columns = strings.Join(iter.columns, ",")
	}

	whereClause := ""
	args := make([]any, 0, 1)
	if iter.lastProcessedVal != nil {
		whereClause = fmt.Sprintf(whereClauseFmt, iter.orderingColumn)
		args = append(args, iter.lastProcessedVal)
	}

	query := fmt.Sprintf(querySelectRowsFmt, columns, iter.table, whereClause, iter.orderingColumn, iter.batchSize)

	rows, err := iter.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select query %q, %v: %w", query, args, err)
	}

	iter.rows = rows

	return nil
}

// populates keyColumn from the database's metadata
// or from the orderingColumn configuration field.
func (iter *Iterator) populateKeyColumns(ctx context.Context) error {
	if len(iter.keyColumns) != 0 {
		return nil
	}

	rows, err := iter.db.QueryxContext(ctx, querySelectPKs, iter.table, iter.database)
	if err != nil {
		return fmt.Errorf("select primary keys: %w", err)
	}
	defer rows.Close()

	keyColumn := ""
	for rows.Next() {
		if err = rows.Scan(&keyColumn); err != nil {
			return fmt.Errorf("scan key column value: %w", err)
		}

		iter.keyColumns = append(iter.keyColumns, keyColumn)
	}

	if len(iter.keyColumns) != 0 {
		return nil
	}

	iter.keyColumns = []string{iter.orderingColumn}

	return nil
}
