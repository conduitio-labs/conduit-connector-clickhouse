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

package source

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-clickhouse/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"go.uber.org/multierr"

	// Go driver for ClickHouse.
	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// metadata related.
const (
	metadataTable = "clickhouse.table"

	querySelectRowsFmt = "SELECT %s FROM %s%s ORDER BY %s ASC LIMIT %d;"
	whereClauseFmt     = " WHERE %s > ?"
)

// Source is a ClickHouse source plugin.
type Source struct {
	sdk.UnimplementedSource

	config config.Source
	db     *sqlx.DB
	rows   *sqlx.Rows

	// last processed orderingColumn value
	lastProcessedVal any
}

// NewSource initialises a new source.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.URL: {
			Default:     "",
			Required:    true,
			Description: "DSN to connect to the database.",
		},
		config.Table: {
			Default:     "",
			Required:    true,
			Description: "Name of the table that the connector should read.",
		},
		config.KeyColumns: {
			Default:  "",
			Required: true,
			Description: "Comma-separated list of column names to build the sdk.Record.Key. " +
				"Column names are the keys of the sdk.Record.Key map, and the values are taken from the row.",
		},
		config.OrderingColumn: {
			Default:  "",
			Required: true,
			Description: "Column name that the connector will use for ordering rows. Column must contain unique " +
				"values and suitable for sorting, otherwise the snapshot won't work correctly.",
		},
		config.Columns: {
			Default:  "",
			Required: false,
			Description: "Comma-separated list of column names that should be included in each payload of the " +
				"sdk.Record. By default includes all columns.",
		},
		config.BatchSize: {
			Default:     "1000",
			Required:    false,
			Description: "Size of rows batch. Min is 1 and max is 100000. The default is 1000.",
		},
	}
}

// Configure parses and stores configurations,
// returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfgRaw map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring ClickHouse Source...")

	cfg, err := config.ParseSource(cfgRaw)
	if err != nil {
		return fmt.Errorf("parse source config: %w", err)
	}

	s.config = cfg

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening a ClickHouse Source...")

	if position != nil {
		if err := json.Unmarshal(position, &s.lastProcessedVal); err != nil {
			return fmt.Errorf("unmarshal sdk.Position into Position: %w", err)
		}
	}

	db, err := sqlx.Open("clickhouse", s.config.URL)
	if err != nil {
		return fmt.Errorf("open db connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	s.db = db

	err = s.loadRows(ctx)
	if err != nil {
		return fmt.Errorf("load rows: %w", err)
	}

	return nil
}

// Read returns the next record.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	sdk.Logger(ctx).Debug().Msg("Reading a record from ClickHouse Source...")

	hasNext, err := s.hasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	row := make(map[string]any)

	err = s.rows.MapScan(row)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	if _, ok := row[s.config.OrderingColumn]; !ok {
		return sdk.Record{}, fmt.Errorf("ordering column %q not found", s.config.OrderingColumn)
	}

	key := make(sdk.StructuredData)
	for i := range s.config.KeyColumns {
		val, ok := row[s.config.KeyColumns[i]]
		if !ok {
			return sdk.Record{}, fmt.Errorf("key column %q not found", s.config.KeyColumns[i])
		}

		key[s.config.KeyColumns[i]] = val
	}

	rowBytes, err := json.Marshal(row)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	// set a new position into the variable,
	// to avoid saving position into the struct until we marshal the position
	positionBytes, err := json.Marshal(row[s.config.OrderingColumn])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	s.lastProcessedVal = row[s.config.OrderingColumn]

	metadata := sdk.Metadata{
		metadataTable: s.config.Table,
	}
	metadata.SetCreatedAt(time.Now())

	return sdk.Util.Source.NewRecordCreate(
		positionBytes,
		metadata,
		key,
		sdk.RawData(rowBytes),
	), nil
}

// Ack appends the last processed value to the slice to clear the tracking table in the future.
func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")

	return nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) (err error) {
	sdk.Logger(ctx).Info().Msg("Tearing down the ClickHouse Source")

	if s.rows != nil {
		err = s.rows.Close()
	}

	if s.db != nil {
		err = multierr.Append(err, s.db.Close())
	}

	return
}

// returns a bool indicating whether the source has the next record to return or not.
func (s *Source) hasNext(ctx context.Context) (bool, error) {
	if s.rows != nil && s.rows.Next() {
		return true, nil
	}

	if err := s.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return s.rows.Next(), nil
}

// selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (s *Source) loadRows(ctx context.Context) error {
	columns := "*"
	if len(s.config.Columns) > 0 {
		columns = strings.Join(s.config.Columns, ",")
	}

	whereClause := ""
	args := make([]any, 0, 1)
	if s.lastProcessedVal != nil {
		whereClause = fmt.Sprintf(whereClauseFmt, s.config.OrderingColumn)
		args = append(args, s.lastProcessedVal)
	}

	query := fmt.Sprintf(querySelectRowsFmt,
		columns, s.config.Table, whereClause, s.config.OrderingColumn, s.config.BatchSize)

	rows, err := s.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select query %q, %v: %w", query, args, err)
	}

	s.rows = rows

	return nil
}
