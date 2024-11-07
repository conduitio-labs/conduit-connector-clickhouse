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

//go:generate mockgen -destination mock/destination.go -package mock . Writer

package destination

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	cconfig "github.com/conduitio-labs/conduit-connector-clickhouse/config"
	"github.com/conduitio-labs/conduit-connector-clickhouse/destination/writer"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

// driverName is a database driver name.
const driverName = "clickhouse"

type engineName string

const (
	mergeTree                    engineName = "MergeTree"
	replacingMergeTree           engineName = "ReplacingMergeTree"
	sharedReplacingMergeTree     engineName = "SharedReplacingMergeTree"
	summingMergeTree             engineName = "SummingMergeTree"
	aggregatingMergeTree         engineName = "AggregatingMergeTree"
	collapsingMergeTree          engineName = "CollapsingMergeTree"
	versionedCollapsingMergeTree engineName = "VersionedCollapsingMergeTree"
	graphiteMergeTree            engineName = "GraphiteMergeTree"
	distributed                  engineName = "Distributed"
	merge                        engineName = "Merge"
)

var engines = map[engineName]struct{}{
	mergeTree:                    {},
	replacingMergeTree:           {},
	sharedReplacingMergeTree:     {},
	summingMergeTree:             {},
	aggregatingMergeTree:         {},
	collapsingMergeTree:          {},
	versionedCollapsingMergeTree: {},
	graphiteMergeTree:            {},
	distributed:                  {},
	merge:                        {},
}

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	Insert(context.Context, opencdc.Record) error
	Update(context.Context, opencdc.Record) error
	Delete(context.Context, opencdc.Record) error
}

// Destination is a Clickhouse destination plugin.
type Destination struct {
	sdk.UnimplementedDestination

	config cconfig.DestinationConfig
	db     *sqlx.DB
	writer Writer
}

// NewDestination initialises a new Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the SourceConfig.
func (d *Destination) Parameters() config.Parameters {
	return d.config.Parameters()
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (d *Destination) Configure(ctx context.Context, cfg config.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring ClickHouse DestinationConfig...")

	var destConfig cconfig.DestinationConfig
	err := sdk.Util.ParseConfig(ctx, cfg, &destConfig, NewDestination().Parameters())
	if err != nil {
		return err
	}
	d.config = destConfig

	return nil
}

// Open initializes a publisher client.
func (d *Destination) Open(ctx context.Context) (err error) {
	sdk.Logger(ctx).Info().Msg("Opening a ClickHouse Destination...")

	db, err := sqlx.Open(driverName, d.config.URL)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}

	d.db = db

	supportsMutations, err := d.checkSupportMutations(ctx)
	if err != nil {
		return fmt.Errorf("support mutations: %w", err)
	}

	d.writer, err = writer.NewWriter(ctx, writer.Params{
		DB:                db,
		Table:             d.config.Table,
		KeyColumns:        d.config.KeyColumns,
		SupportsMutations: supportsMutations,
	})
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
}

// Write writes records into a Destination.
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
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

// checkSupportMutations checks that the table engine supports update or delete operations.
func (d *Destination) checkSupportMutations(ctx context.Context) (bool, error) {
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

	_, supportsMutations := engines[engineName(engine)]

	return supportsMutations, nil
}
