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

package destination

import (
	"context"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-clickhouse/config"
	"github.com/conduitio-labs/conduit-connector-clickhouse/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"

	// Go driver for ClickHouse.
	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	Insert(context.Context, sdk.Record) error
	Update(context.Context, sdk.Record) error
	Delete(context.Context, sdk.Record) error
}

// A Destination represents the destination connector.
type Destination struct {
	sdk.UnimplementedDestination

	db     *sqlx.DB
	writer Writer
	cfg    config.General
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
			Description: "The connection string to connect to ClickHouse database.",
		},
		config.Table: {
			Default:     "",
			Required:    true,
			Description: "The table name of the table in ClickHouse that the connector should write to, by default.",
		},
	}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (d *Destination) Configure(_ context.Context, cfg map[string]string) (err error) {
	d.cfg, err = config.Parse(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	return nil
}

// Open initializes a publisher client.
func (d *Destination) Open(ctx context.Context) (err error) {
	db, err := sqlx.Open("clickhouse", d.cfg.URL)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	d.writer, err = writer.NewWriter(ctx, writer.Params{
		DB:    db,
		Table: d.cfg.Table,
	})
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
}

// Write writes records into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i, record := range records {
		err := sdk.Util.Destination.Route(ctx, record,
			d.writer.Insert,
			d.writer.Update,
			d.writer.Delete,
			d.writer.Insert,
		)
		if err != nil {
			return i, fmt.Errorf("route %s: %w", record.Operation.String(), err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.db != nil {
		return d.db.Close()
	}

	return nil
}
