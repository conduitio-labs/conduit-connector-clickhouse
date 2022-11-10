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

package destination

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/conduitio-labs/conduit-connector-clickhouse/config"
	"github.com/conduitio-labs/conduit-connector-clickhouse/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	Insert(context.Context, sdk.Record) error
	Update(context.Context, sdk.Record) error
	Delete(context.Context, sdk.Record) error
}

// Destination is a Clickhouse destination plugin.
type Destination struct {
	sdk.UnimplementedDestination

	config config.Destination
	db     *sqlx.DB
	writer Writer
}

// NewDestination initialises a new Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.URL: {
			Default:     "",
			Required:    true,
			Description: "DSN to connect to the database.",
		},
		config.Table: {
			Default:     "",
			Required:    true,
			Description: "Name of the table that the connector should write to.",
		},
		config.KeyColumns: {
			Default:     "",
			Required:    false,
			Description: "Comma-separated list of column names for key handling. ",
		},
	}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) (err error) {
	sdk.Logger(ctx).Info().Msg("Configuring ClickHouse Destination...")

	d.config, err = config.ParseDestination(cfg)
	if err != nil {
		return fmt.Errorf("parse destination config: %w", err)
	}

	return nil
}

// Open initializes a publisher client.
func (d *Destination) Open(ctx context.Context) (err error) {
	sdk.Logger(ctx).Info().Msg("Opening a ClickHouse Destination...")

	db, err := sqlx.Open("clickhouse", d.config.URL)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	d.db = db

	isSupportMutations, err := d.checkSupportMutations(ctx)
	if err != nil {
		return fmt.Errorf("support mutations: %w", err)
	}

	d.writer, err = writer.NewWriter(ctx, writer.Params{
		DB:                 db,
		Table:              d.config.Table,
		KeyColumns:         d.config.KeyColumns,
		IsSupportMutations: isSupportMutations,
	})
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
}

// Write writes records into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i := range records {
		sdk.Logger(ctx).Debug().Bytes("record", records[i].Bytes()).
			Msg("Writing a record into ClickHouse Destination...")

		err := sdk.Util.Destination.Route(ctx, records[i],
			d.writer.Insert,
			d.writer.Update,
			d.writer.Delete,
			d.writer.Insert,
		)
		if err != nil {
			if records[i].Key != nil {
				return i, fmt.Errorf("key %s: %w", string(records[i].Key.Bytes()), err)
			}

			return i, fmt.Errorf("record with no key: %w", err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down the ClickHouse Destination")

	if d.db != nil {
		return d.db.Close()
	}

	return nil
}

func (d *Destination) checkSupportMutations(ctx context.Context) (bool, error) {
	engines := map[string]struct{}{
		"MergeTree":                    {},
		"ReplacingMergeTree":           {},
		"SummingMergeTree":             {},
		"AggregatingMergeTree":         {},
		"CollapsingMergeTree":          {},
		"VersionedCollapsingMergeTree": {},
		"GraphiteMergeTree":            {},
		"Distributed":                  {},
		"Merge":                        {},
	}

	options, err := clickhouse.ParseDSN(d.config.URL)
	if err != nil {
		return false, fmt.Errorf("parse dsn: %w", err)
	}

	row := d.db.QueryRowxContext(ctx, "SELECT engine FROM system.tables WHERE database=? and name=?;",
		options.Auth.Database, d.config.Table)
	if err != nil {
		return false, fmt.Errorf("select table engine: %w", err)
	}

	engine := ""
	err = row.Scan(&engine)
	if err != nil {
		return false, fmt.Errorf("scan table engine: %w", err)
	}

	_, isSupportMutations := engines[engine]

	return isSupportMutations, nil
}
