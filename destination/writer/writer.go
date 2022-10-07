// Copyright © 2022 Meroxa, Inc.
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

package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-clickhouse/columntypes"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

const (
	// metadata key with the name of the table.
	metadataTable = "clickhouse.table"

	// query patterns.
	insertFmt = "INSERT INTO %s (%s) VALUES (%s)"
	updateFmt = "ALTER TABLE %s UPDATE %s WHERE %s"
	deleteFmt = "ALTER TABLE %s DELETE WHERE %s"

	coma        = ","
	placeholder = "?"
	operatorAnd = " AND "
)

// Writer implements a writer logic for ClickHouse destination.
type Writer struct {
	db             *sqlx.DB
	table          string
	primaryColumns []string
	columnTypes    map[string]string
}

// Params is an incoming params for the New function.
type Params struct {
	DB             *sqlx.DB
	Table          string
	PrimaryColumns []string
}

// New creates new instance of the Writer.
func New(ctx context.Context, params Params) (*Writer, error) {
	writer := &Writer{
		db:             params.DB,
		table:          params.Table,
		primaryColumns: params.PrimaryColumns,
	}

	columnTypes, err := columntypes.GetColumnTypes(ctx, writer.db, writer.table)
	if err != nil {
		return nil, fmt.Errorf("get column types: %w", err)
	}

	writer.columnTypes = columnTypes

	return writer, nil
}

// Write writes a sdk.Record into a Destination.
func (w *Writer) Write(ctx context.Context, record sdk.Record) error {
	switch record.Operation {
	case sdk.OperationUpdate:
		return w.update(ctx, record)
	case sdk.OperationDelete:
		return w.delete(ctx, record)
	default:
		return w.insert(ctx, record)
	}
}

// inserts a record.
func (w *Writer) insert(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return errNoPayload
	}

	payload, err = columntypes.ConvertStructureData(w.columnTypes, payload)
	if err != nil {
		return fmt.Errorf("convert structure data: %w", err)
	}

	key, err := w.structurizeData(record.Key)
	if err != nil {
		// if the key is not structured, we simply ignore it
		// we'll try to insert just a payload in this case
		sdk.Logger(ctx).Debug().Msgf("structurize key during insert: %v", err)
	}

	keyColumns, err := w.getKeyColumns(key)
	if err != nil {
		return fmt.Errorf("get key columns: %w", err)
	}

	// if the record doesn't contain some key column, insert it
	for i := range keyColumns {
		if _, ok := payload[keyColumns[i]]; !ok {
			payload[keyColumns[i]] = key[keyColumns[i]]
		}
	}

	columns, values := w.extractColumnsAndValues(payload)

	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = placeholder
	}

	query := fmt.Sprintf(insertFmt, tableName, strings.Join(columns, coma), strings.Join(placeholders, coma))

	_, err = w.db.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("exec insert %q, %v: %w", query, values, err)
	}

	return nil
}

// updates a record.
func (w *Writer) update(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return errNoPayload
	}

	payload, err = columntypes.ConvertStructureData(w.columnTypes, payload)
	if err != nil {
		return fmt.Errorf("convert structure data: %w", err)
	}

	key, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	keyColumns, err := w.getKeyColumns(key)
	if err != nil {
		return fmt.Errorf("get key columns: %w", err)
	}

	// remove keys from the payload
	for i := range keyColumns {
		delete(payload, keyColumns[i])
	}

	columns, values := w.extractColumnsAndValues(payload)

	updateColumns := make([]string, len(columns))
	for i := range columns {
		updateColumns[i] = fmt.Sprintf("%s = %s", columns[i], placeholder)
	}

	whereClauses := make([]string, len(keyColumns))
	for i := range keyColumns {
		whereClauses[i] = fmt.Sprintf("%s = %s", keyColumns[i], placeholder)
		values = append(values, key[keyColumns[i]])
	}

	query := fmt.Sprintf(updateFmt, tableName, strings.Join(updateColumns, coma), strings.Join(whereClauses, operatorAnd))

	_, err = w.db.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("exec update %q, %v: %w", query, values, err)
	}

	return nil
}

// deletes a record.
func (w *Writer) delete(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	key, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	keyColumns, err := w.getKeyColumns(key)
	if err != nil {
		return fmt.Errorf("get key columns: %w", err)
	}

	whereClauses := make([]string, len(keyColumns))
	values := make([]any, 0, len(keyColumns))
	for i := range keyColumns {
		whereClauses[i] = fmt.Sprintf("%s = %s", keyColumns[i], placeholder)
		values = append(values, key[keyColumns[i]])
	}

	query := fmt.Sprintf(deleteFmt, tableName, strings.Join(whereClauses, operatorAnd))

	_, err = w.db.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("exec delete %q, %v: %w", query, values, err)
	}

	return nil
}

// returns either the record metadata value for the table
// or the default configured value for the table.
func (w *Writer) getTableName(metadata map[string]string) string {
	tableName, ok := metadata[metadataTable]
	if !ok {
		return w.table
	}

	return tableName
}

// returns either all the keys of the sdk.Record's Key field,
// or only the keys, which were set in the primaryColumns configuration field.
func (w *Writer) getKeyColumns(key sdk.StructuredData) ([]string, error) {
	keyColumns := make([]string, 0, len(w.primaryColumns))

	if len(w.primaryColumns) > 0 {
		for i := range w.primaryColumns {
			if val, ok := key[w.primaryColumns[i]]; !ok && val != nil {
				return nil, fmt.Errorf("the primary key %q is not found in the Key of sdk.Record, or has a null value",
					w.primaryColumns[i])
			}

			keyColumns = append(keyColumns, w.primaryColumns[i])
		}

		return keyColumns, nil
	}

	for k := range key {
		keyColumns = append(keyColumns, k)
	}

	return keyColumns, nil
}

// converts sdk.Data to sdk.StructuredData.
func (w *Writer) structurizeData(data sdk.Data) (sdk.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, nil
	}

	structuredData := make(sdk.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}

// turns the payload into slices of columns and values for inserting into ClickHouse.
func (w *Writer) extractColumnsAndValues(payload sdk.StructuredData) ([]string, []any) {
	var (
		columns []string
		values  []any
	)

	for key, value := range payload {
		columns = append(columns, key)
		values = append(values, value)
	}

	return columns, values
}
