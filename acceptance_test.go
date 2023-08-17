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

package clickhouse

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/conduitio-labs/conduit-connector-clickhouse/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
)

const (
	// driverName is a database driver name.
	driverName = "clickhouse"
	// envNameURL is a ClickHouse url environment name.
	envNameURL = "CLICKHOUSE_URL"
	// metadataFieldTable is a name of a record metadata field that stores a ClickHouse table name.
	metadataFieldTable = "clickhouse.table"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	id int32
}

// GenerateRecord generates a random sdk.Record.
func (d *driver) GenerateRecord(_ *testing.T, operation sdk.Operation) sdk.Record {
	atomic.AddInt32(&d.id, 1)

	return sdk.Record{
		Position:  nil,
		Operation: operation,
		Metadata: map[string]string{
			metadataFieldTable: d.Config.SourceConfig[config.Table],
		},
		Key: sdk.StructuredData{
			"Int32Type": d.id,
		},
		Payload: sdk.Change{After: sdk.RawData(
			fmt.Sprintf(`{"Int32Type":%d,"StringType":"%s"}`, d.id, uuid.NewString()),
		)},
	}
}

func TestAcceptance(t *testing.T) {
	cfg := prepareConfig(t)

	is := is.New(t)

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest: func(t *testing.T) {
					err := createTable(cfg[config.URL], cfg[config.Table])
					is.NoErr(err)
				},
				AfterTest: func(t *testing.T) {
					err := dropTables(cfg[config.URL], cfg[config.Table])
					is.NoErr(err)
				},
			},
		},
	})
}

// prepareConfig receives the connection URL from the environment variable
// and prepares configuration map.
func prepareConfig(t *testing.T) map[string]string {
	url := os.Getenv(envNameURL)
	if url == "" {
		t.Skipf("%s env var must be set", envNameURL)

		return nil
	}

	return map[string]string{
		config.URL:            url,
		config.Table:          fmt.Sprintf("CONDUIT_TEST_%s", randString(6)),
		config.KeyColumns:     "Int32Type",
		config.OrderingColumn: "Int32Type",
		config.Snapshot:       "true",
		config.BatchSize:      "1000",
	}
}

// createTable creates test table.
func createTable(url, table string) error {
	db, err := sqlx.Open(driverName, url)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf(`
	CREATE TABLE %s
	(
		Int32Type Int32,
		StringType String
	) ENGINE ReplacingMergeTree() PRIMARY KEY Int32Type;`, table))
	if err != nil {
		return fmt.Errorf("execute create table query: %w", err)
	}

	return nil
}

// dropTables drops test table and tracking test table if exists.
func dropTables(url, table string) error {
	db, err := sqlx.Open(driverName, url)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", table))
	if err != nil {
		return fmt.Errorf("execute drop table query: %w", err)
	}

	return nil
}

// generates a random string of length n.
func randString(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck // does not actually fail

	return hex.EncodeToString(b)
}
