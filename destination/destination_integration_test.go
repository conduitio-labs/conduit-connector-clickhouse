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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"

	"github.com/conduitio-labs/conduit-connector-clickhouse/config"
)

func TestDestination_Write_successInsert(t *testing.T) {
	type dataRow struct {
		intType         int
		stringType      string
		floatType       float32
		doubleType      float64
		booleanType     bool
		uuidType        uuid.UUID
		dateType        time.Time
		datetimeType    time.Time
		arrayInt32Type  []int32
		arrayStringType []string
		mapType         map[string]int32
	}

	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	db, err := sqlx.Open("clickhouse", cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = db.Ping()
	is.NoErr(err)

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

	want := dataRow{
		intType:         42,
		stringType:      "John",
		floatType:       float32(123.45),
		doubleType:      123.45,
		booleanType:     true,
		uuidType:        uuid.New(),
		dateType:        time.Date(2009, 11, 10, 0, 0, 0, 0, time.UTC),
		datetimeType:    time.Date(2009, 11, 10, 23, 0, 0, 0, time.UTC),
		arrayInt32Type:  []int32{10, 20, 30},
		arrayStringType: []string{"test_a", "test_b", "test_c"},
		mapType: map[string]int32{
			"test1": 1,
			"test2": 2,
		},
	}

	record := sdk.Record{
		Operation: sdk.OperationSnapshot,
		Payload: sdk.Change{After: sdk.StructuredData{
			"Int32Type":       want.intType,
			"StringType":      want.stringType,
			"FloatType":       want.floatType,
			"DoubleType":      want.doubleType,
			"BooleanType":     want.booleanType,
			"UUIDType":        want.uuidType,
			"DateType":        want.dateType,
			"DatetimeType":    want.datetimeType,
			"ArrayInt32Type":  want.arrayInt32Type,
			"ArrayStringType": want.arrayStringType,
			"MapType":         want.mapType,
		}},
	}

	n, err := dest.Write(ctx, []sdk.Record{record})
	is.NoErr(err)
	is.Equal(n, 1)

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)

	// wait a bit to be sure that the data have been recorded
	time.Sleep(time.Second)

	res := dataRow{}
	err = db.QueryRow(fmt.Sprintf("SELECT * FROM %s WHERE Int32Type = %d;", cfg[config.Table], 42)).
		Scan(&res.intType,
			&res.stringType,
			&res.floatType,
			&res.doubleType,
			&res.booleanType,
			&res.uuidType,
			&res.dateType,
			&res.datetimeType,
			&res.arrayInt32Type,
			&res.arrayStringType,
			&res.mapType)
	is.NoErr(err)

	// time with the location set to UTC
	res.dateType = res.dateType.UTC()
	res.datetimeType = res.datetimeType.UTC()
	is.Equal(res, want)
}

func TestDestination_Write_successUpdate(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	db, err := sqlx.Open("clickhouse", cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = db.Ping()
	is.NoErr(err)

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

	n, err := dest.Write(ctx, []sdk.Record{
		{
			Operation: sdk.OperationUpdate,
			Key: sdk.StructuredData{
				"Int32Type": 42,
			},
			Payload: sdk.Change{After: sdk.StructuredData{
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
	n, err = dest.Write(ctx, []sdk.Record{
		{
			Operation: sdk.OperationUpdate,
			Payload: sdk.Change{After: sdk.StructuredData{
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

func TestDestination_Write_successDelete(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	db, err := sqlx.Open("clickhouse", cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = db.Ping()
	is.NoErr(err)

	err = createTable(db, cfg[config.Table])
	is.NoErr(err)

	defer func() {
		err = dropTable(db, cfg[config.Table])
		is.NoErr(err)
	}()

	err = insertData(db, cfg[config.Table])
	is.NoErr(err)

	// check if row exists
	_, err = getStringFieldByIntField(db, cfg[config.Table], 42)
	is.NoErr(err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	n, err := dest.Write(ctx, []sdk.Record{
		{
			Operation: sdk.OperationDelete,
			Key:       sdk.RawData(`{"Int32Type":42}`),
		},
	})
	is.NoErr(err)
	is.Equal(n, 1)

	_, err = getStringFieldByIntField(db, cfg[config.Table], 42)
	is.Equal(err.Error(), "scan row: sql: no rows in result set")

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write_failedWrongColumn(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	db, err := sqlx.Open("clickhouse", cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = db.Ping()
	is.NoErr(err)

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

	_, err = dest.Write(ctx, []sdk.Record{
		{
			Operation: sdk.OperationSnapshot,
			Payload: sdk.Change{After: sdk.StructuredData{
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

	db, err := sqlx.Open("clickhouse", cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = db.Ping()
	is.NoErr(err)

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
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			cfg[config.Table] = tt.table

			cctx, cancel := context.WithCancel(ctx)

			dest := NewDestination()

			err = dest.Configure(cctx, cfg)
			is.NoErr(err)

			err = dest.Open(cctx)
			is.NoErr(err)

			// OperationCreate
			n, err := dest.Write(cctx, []sdk.Record{
				{
					Operation: sdk.OperationCreate,
					Key: sdk.StructuredData{
						"Int32Type": 42,
					},
					Payload: sdk.Change{After: sdk.StructuredData{
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
			n, err = dest.Write(cctx, []sdk.Record{
				{
					Operation: sdk.OperationUpdate,
					Key: sdk.StructuredData{
						"Int32Type": 42,
					},
					Payload: sdk.Change{After: sdk.StructuredData{
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
			n, err = dest.Write(cctx, []sdk.Record{
				{
					Operation: sdk.OperationDelete,
					Key:       sdk.RawData(`{"Int32Type":42}`),
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

func prepareConfig(t *testing.T) map[string]string {
	url := os.Getenv("CLICKHOUSE_URL")
	if url == "" {
		t.Skip("CLICKHOUSE_URL env var must be set")

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
