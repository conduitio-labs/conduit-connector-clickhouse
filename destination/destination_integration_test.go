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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-clickhouse/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
)

// envNameURL is a ClickHouse url environment name.
const envNameURL = "CLICKHOUSE_URL"

func TestDestination_Write_successCheckEngines(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	tests := []struct {
		name             string
		table            string
		createQuery      string
		supportMutations bool
		mutationErrMsg   string
	}{
		{
			name:  "merge_tree",
			table: fmt.Sprintf("%s_MergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE MergeTree() PRIMARY KEY Int32Type;`,
			supportMutations: true,
		}, {
			name:  "replacing_merge_tree",
			table: fmt.Sprintf("%s_ReplacingMergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE ReplacingMergeTree() PRIMARY KEY Int32Type;`,
			supportMutations: true,
		}, {
			name:  "summing_merge_tree",
			table: fmt.Sprintf("%s_SummingMergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE SummingMergeTree() PRIMARY KEY Int32Type;`,
			supportMutations: true,
		}, {
			name:  "aggregating_merge_tree",
			table: fmt.Sprintf("%s_AggregatingMergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE AggregatingMergeTree() PRIMARY KEY Int32Type;`,
			supportMutations: true,
		}, {
			name:  "collapsing_merge_tree",
			table: fmt.Sprintf("%s_CollapsingMergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String,
				Sign		Int8	DEFAULT 1
			) ENGINE CollapsingMergeTree(Sign) PRIMARY KEY Int32Type;`,
			supportMutations: true,
		}, {
			name:  "versioned_collapsing_merge_tree",
			table: fmt.Sprintf("%s_VersionedCollapsingMergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
		(
			Int32Type	Int32,
			StringType	String,
			Sign		Int8,
    		Version		UInt8
		) ENGINE VersionedCollapsingMergeTree(Sign, Version) PRIMARY KEY Int32Type;`,
			supportMutations: true,
		},
		// to run this test case add to the clickhouse server config next lines:
		// <graphite_rollup>
		//   <path_column_name>Path</path_column_name>
		//   <time_column_name>Time</time_column_name>
		//   <value_column_name>Value</value_column_name>
		//   <version_column_name>Version</version_column_name>
		// </graphite_rollup>
		{
			name:  "graphite_merge_tree",
			table: fmt.Sprintf("%s_GraphiteMergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
		(
			Int32Type	Int32,
			StringType	String,
			Path		String,
			Time		DateTime,
			Value		Int64,
			Version		Int32
		) ENGINE GraphiteMergeTree('graphite_rollup') PRIMARY KEY Int32Type;`,
			supportMutations: true,
		},
		{
			name:  "tiny_log",
			table: fmt.Sprintf("%s_TinyLog", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE TinyLog();`,
			supportMutations: false,
			mutationErrMsg:   "Table engine TinyLog doesn't support mutations",
		},
		{
			name:  "stripe_log",
			table: fmt.Sprintf("%s_StripeLog", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE StripeLog();`,
			supportMutations: false,
			mutationErrMsg:   "Table engine StripeLog doesn't support mutations",
		},
		{
			name:  "log",
			table: fmt.Sprintf("%s_Log", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE Log();`,
			supportMutations: false,
			mutationErrMsg:   "Table engine Log doesn't support mutations",
		},
	}

	db, err := sqlx.Open(driverName, cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	for i := range tests {
		_, err = db.Exec(fmt.Sprintf(tests[i].createQuery, tests[i].table))
		is.NoErr(err)
	}

	defer func() {
		for i := range tests {
			err = dropTable(db, tests[i].table)
			is.NoErr(err)
		}
	}()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			cfg[config.Table] = tt.table

			cctx, cancel := context.WithCancel(ctx)

			dest := NewDestination()

			err = dest.Configure(cctx, cfg)
			is.NoErr(err)

			err = dest.Open(cctx)
			is.NoErr(err)

			// OperationCreate
			n, err := dest.Write(cctx, []opencdc.Record{
				{
					Operation: opencdc.OperationCreate,
					Key: opencdc.StructuredData{
						"Int32Type": 42,
					},
					Payload: opencdc.Change{After: opencdc.StructuredData{
						"Int32Type":  42,
						"StringType": "Jane",
					}},
				},
			})
			is.NoErr(err)
			is.Equal(n, 1)

			name, err := getStringFieldByIntField(db, cfg[config.Table], 42)
			is.NoErr(err)
			is.Equal(name, "Jane")

			// OperationUpdate
			n, err = dest.Write(cctx, []opencdc.Record{
				{
					Operation: opencdc.OperationUpdate,
					Key: opencdc.StructuredData{
						"Int32Type": 42,
					},
					Payload: opencdc.Change{After: opencdc.StructuredData{
						"Int32Type":  42,
						"StringType": "Sam",
					}},
				},
			})
			is.NoErr(err)
			is.Equal(n, 1)

			if tt.supportMutations {
				name, err = getStringFieldByIntField(db, cfg[config.Table], 42)
				is.NoErr(err)
				is.Equal(name, "Sam")
			}

			// OperationDelete
			n, err = dest.Write(cctx, []opencdc.Record{
				{
					Operation: opencdc.OperationDelete,
					Key:       opencdc.RawData(`{"Int32Type":42}`),
				},
			})
			is.NoErr(err)
			is.Equal(n, 1)

			if tt.supportMutations {
				name, err = getStringFieldByIntField(db, cfg[config.Table], 42)
				is.True(err != nil)
				is.Equal(name, "")
			}

			cancel()

			err = dest.Teardown(context.Background())
			is.NoErr(err)
		})
	}
}

func TestDestination_Write_successKeyColumns(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	db, err := sqlx.Open(driverName, cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = createTable(db, cfg[config.Table])
	is.NoErr(err)

	defer func() {
		err = dropTable(db, cfg[config.Table])
		is.NoErr(err)
	}()

	err = insertData(db, cfg[config.Table])
	is.NoErr(err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// set a KeyColumns field to the config
	cfg[config.KeyColumns] = "Int32Type"

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	// update the record with a Key
	n, err := dest.Write(ctx, []opencdc.Record{
		{
			Operation: opencdc.OperationUpdate,
			Key: opencdc.StructuredData{
				"Int32Type": 42,
			},
			Payload: opencdc.Change{After: opencdc.StructuredData{
				"Int32Type":  42,
				"StringType": "Jane",
			}},
		},
	})
	is.NoErr(err)
	is.Equal(n, 1)

	name, err := getStringFieldByIntField(db, cfg[config.Table], 42)
	is.NoErr(err)
	is.Equal(name, "Jane")

	// update the record with no Key
	n, err = dest.Write(ctx, []opencdc.Record{
		{
			Operation: opencdc.OperationUpdate,
			Payload: opencdc.Change{After: opencdc.StructuredData{
				"Int32Type":  42,
				"StringType": "Sam",
			}},
		},
	})
	is.NoErr(err)
	is.Equal(n, 1)

	name, err = getStringFieldByIntField(db, cfg[config.Table], 42)
	is.NoErr(err)
	is.Equal(name, "Sam")

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write_failedWrongKeyColumnsField(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	db, err := sqlx.Open(driverName, cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = createTable(db, cfg[config.Table])
	is.NoErr(err)

	defer func() {
		err = dropTable(db, cfg[config.Table])
		is.NoErr(err)
	}()

	err = insertData(db, cfg[config.Table])
	is.NoErr(err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// set a wrong KeyColumns field to the config
	cfg[config.KeyColumns] = "wrong_column"

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	// update the record with no Key
	_, err = dest.Write(ctx, []opencdc.Record{
		{
			Operation: opencdc.OperationUpdate,
			Payload: opencdc.Change{After: opencdc.StructuredData{
				"Int32Type":  42,
				"StringType": "Sam",
			}},
		},
	})
	is.Equal(err.Error(), "record with no key: key column \"wrong_column\" not found")

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write_failedWrongPayloadKey(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	db, err := sqlx.Open(driverName, cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = createTable(db, cfg[config.Table])
	is.NoErr(err)

	defer func() {
		err = dropTable(db, cfg[config.Table])
		is.NoErr(err)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	_, err = dest.Write(ctx, []opencdc.Record{
		{
			Operation: opencdc.OperationSnapshot,
			Payload: opencdc.Change{After: opencdc.StructuredData{
				"Int32Type":    43,
				"wrong_column": "test",
			}},
		},
	})
	is.True(strings.Contains(err.Error(), "record with no key: exec insert"))

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

func prepareConfig(t *testing.T) map[string]string {
	url := os.Getenv(envNameURL)
	if url == "" {
		t.Skipf("%s env var must be set", envNameURL)

		return nil
	}

	return map[string]string{
		config.URL:   url,
		config.Table: fmt.Sprintf("CONDUIT_DEST_TEST_%s", randString(6)),
	}
}

func createTable(db *sqlx.DB, table string) error {
	_, err := db.Exec(fmt.Sprintf(`
	CREATE TABLE %s
	(
		Int32Type		Int32,
		StringType		String,
		FloatType		Float32,
		DoubleType		Float64,
		BooleanType		Bool,
		UUIDType		UUID,
		DateType		Date,
		DatetimeType	DateTime,
		ArrayInt32Type	Array(Int32),
		ArrayStringType	Array(String),
		MapType			Map(String, Int32)
	) ENGINE ReplacingMergeTree() PRIMARY KEY Int32Type;`, table))
	if err != nil {
		return fmt.Errorf("execute create table query: %w", err)
	}

	return nil
}

func dropTable(db *sqlx.DB, table string) error {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", table))
	if err != nil {
		return fmt.Errorf("execute drop table query: %w", err)
	}

	return nil
}

func insertData(db *sqlx.DB, table string) error {
	_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (Int32Type, StringType) VALUES (42, 'Sam');", table))
	if err != nil {
		return fmt.Errorf("execute insert query: %w", err)
	}

	return nil
}

func getStringFieldByIntField(db *sqlx.DB, table string, id int) (string, error) {
	// wait a bit
	time.Sleep(time.Second)

	row := db.QueryRow(fmt.Sprintf("SELECT StringType FROM %s WHERE Int32Type = %d;", table, id))

	name := ""

	err := row.Scan(&name)
	if err != nil {
		return "", fmt.Errorf("scan row: %w", err)
	}

	return name, nil
}

// generates a random string of length n.
func randString(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck // does not actually fail

	return hex.EncodeToString(b)
}
