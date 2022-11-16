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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/conduitio-labs/conduit-connector-clickhouse/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
)

func TestSource_Read_noTable(t *testing.T) {
	var (
		is = is.New(t)

		keyColumns     = []string{"StringType"}
		orderingColumn = "StringType"
		cfg            = prepareConfig(t, keyColumns, orderingColumn)
	)

	db, err := sqlx.Open("clickhouse", cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = db.Ping()
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.True(strings.Contains(err.Error(), "new iterator: load rows: execute select query"))

	cancel()
}

func TestSource_Read_emptyTable(t *testing.T) {
	var (
		is = is.New(t)

		keyColumns     = []string{"StringType"}
		orderingColumn = "StringType"
		cfg            = prepareConfig(t, keyColumns, orderingColumn)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	_, err = src.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_checkTypes(t *testing.T) {
	type dataRow struct {
		IntType         int              `json:"Int32Type"`
		StringType      string           `json:"StringType"`
		FloatType       float32          `json:"FloatType"`
		DoubleType      float64          `json:"DoubleType"`
		BooleanType     bool             `json:"BooleanType"`
		UUIDType        uuid.UUID        `json:"UUIDType"`
		DateType        time.Time        `json:"DateType"`
		DatetimeType    time.Time        `json:"DatetimeType"`
		ArrayIntType    []int32          `json:"ArrayInt32Type"`
		ArrayStringType []string         `json:"ArrayStringType"`
		MapType         map[string]int32 `json:"MapType"`
	}

	var (
		is = is.New(t)

		keyColumns     = []string{"StringType"}
		orderingColumn = "StringType"
		cfg            = prepareConfig(t, keyColumns, orderingColumn)
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

	want := dataRow{
		IntType:         42,
		StringType:      "John",
		FloatType:       float32(123.45),
		DoubleType:      123.45,
		BooleanType:     true,
		UUIDType:        uuid.New(),
		DateType:        time.Date(2009, 11, 10, 0, 0, 0, 0, time.UTC),
		DatetimeType:    time.Date(2009, 11, 10, 23, 0, 0, 0, time.UTC),
		ArrayIntType:    []int32{10, 20, 30},
		ArrayStringType: []string{"test_a", "test_b", "test_c"},
		MapType: map[string]int32{
			"test1": 1,
			"test2": 2,
		},
	}

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?)", cfg[config.Table]),
		want.IntType,
		want.StringType,
		want.FloatType,
		want.DoubleType,
		want.BooleanType,
		want.UUIDType,
		want.DateType,
		want.DatetimeType,
		clickhouse.ArraySet{want.ArrayIntType},
		clickhouse.ArraySet{want.ArrayStringType},
		want.MapType)
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	record, err := src.Read(ctx)
	is.NoErr(err)

	is.Equal(record.Position, sdk.Position(fmt.Sprintf(`"%s"`, want.StringType)))
	is.Equal(record.Operation, sdk.OperationCreate)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{keyColumns[0]: want.StringType}))

	got := dataRow{}
	err = json.Unmarshal(record.Payload.After.Bytes(), &got)
	is.NoErr(err)

	is.Equal(got.IntType, want.IntType)
	is.Equal(got.StringType, want.StringType)
	is.Equal(got.FloatType, want.FloatType)
	is.Equal(got.DoubleType, want.DoubleType)
	is.Equal(got.BooleanType, want.BooleanType)
	is.Equal(got.UUIDType, want.UUIDType)
	is.Equal(got.DateType.UTC(), want.DateType)
	is.Equal(got.DatetimeType.UTC(), want.DatetimeType)
	is.Equal(got.ArrayIntType, want.ArrayIntType)
	is.Equal(got.ArrayStringType, want.ArrayStringType)
	is.Equal(got.MapType, want.MapType)

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_checkEngines(t *testing.T) {
	type dataRow struct {
		IntType    int    `json:"Int32Type"`
		StringType string `json:"StringType"`
	}

	var (
		is = is.New(t)

		keyColumns     = []string{"StringType"}
		orderingColumn = "StringType"
		cfg            = prepareConfig(t, keyColumns, orderingColumn)
	)

	tests := []struct {
		name        string
		table       string
		createQuery string
	}{
		{
			name:  "merge_tree",
			table: fmt.Sprintf("%s_MergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE MergeTree() PRIMARY KEY Int32Type;`,
		}, {
			name:  "replacing_merge_tree",
			table: fmt.Sprintf("%s_ReplacingMergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE ReplacingMergeTree() PRIMARY KEY Int32Type;`,
		}, {
			name:  "summing_merge_tree",
			table: fmt.Sprintf("%s_SummingMergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE SummingMergeTree() PRIMARY KEY Int32Type;`,
		}, {
			name:  "aggregating_merge_tree",
			table: fmt.Sprintf("%s_AggregatingMergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE AggregatingMergeTree() PRIMARY KEY Int32Type;`,
		}, {
			name:  "collapsing_merge_tree",
			table: fmt.Sprintf("%s_CollapsingMergeTree", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String,
				Sign		Int8	DEFAULT 1
			) ENGINE CollapsingMergeTree(Sign) PRIMARY KEY Int32Type;`,
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
		},
		{
			name:  "tiny_log",
			table: fmt.Sprintf("%s_TinyLog", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE TinyLog();`,
		},
		{
			name:  "stripe_log",
			table: fmt.Sprintf("%s_StripeLog", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE StripeLog();`,
		},
		{
			name:  "log",
			table: fmt.Sprintf("%s_Log", cfg[config.Table]),
			createQuery: `CREATE TABLE %s
			(
				Int32Type	Int32,
				StringType	String
			) ENGINE Log();`,
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

			want := dataRow{
				IntType:    42,
				StringType: "John",
			}

			_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (Int32Type, StringType) VALUES (?, ?)", cfg[config.Table]),
				want.IntType,
				want.StringType)
			is.NoErr(err)

			ctx, cancel := context.WithCancel(context.Background())

			src := NewSource()

			err = src.Configure(ctx, cfg)
			is.NoErr(err)

			err = src.Open(ctx, nil)
			is.NoErr(err)

			record, err := src.Read(ctx)
			is.NoErr(err)

			is.Equal(record.Position, sdk.Position(fmt.Sprintf(`"%s"`, want.StringType)))
			is.Equal(record.Operation, sdk.OperationCreate)
			is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{keyColumns[0]: want.StringType}))

			got := dataRow{}
			err = json.Unmarshal(record.Payload.After.Bytes(), &got)
			is.NoErr(err)

			is.Equal(got.IntType, want.IntType)
			is.Equal(got.StringType, want.StringType)

			cancel()

			err = src.Teardown(context.Background())
			is.NoErr(err)
		})
	}
}

func TestSource_Read_successCombined(t *testing.T) {
	type dataRow struct {
		IntType    int    `json:"Int32Type"`
		StringType string `json:"StringType"`
	}

	var (
		is = is.New(t)

		keyColumns     = []string{"StringType"}
		orderingColumn = "StringType"
		cfg            = prepareConfig(t, keyColumns, orderingColumn)
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

	wants := []dataRow{
		{
			IntType:    345,
			StringType: "abc",
		},
		{
			IntType:    234,
			StringType: "bcd",
		},
		{
			IntType:    456,
			StringType: "cde",
		},
	}

	// insert first two records
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (Int32Type,StringType) VALUES (?,?),(?,?)", cfg[config.Table]),
		wants[0].IntType,
		wants[0].StringType,
		wants[1].IntType,
		wants[1].StringType)
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	// call Read (will return the first record)
	record, err := src.Read(ctx)
	is.NoErr(err)

	is.Equal(record.Position, sdk.Position(fmt.Sprintf(`"%s"`, wants[0].StringType)))
	is.Equal(record.Operation, sdk.OperationCreate)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{keyColumns[0]: wants[0].StringType}))

	got := dataRow{}
	err = json.Unmarshal(record.Payload.After.Bytes(), &got)
	is.NoErr(err)

	is.Equal(got.IntType, wants[0].IntType)
	is.Equal(got.StringType, wants[0].StringType)

	// stop the Source
	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)

	// start a new Source
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	src = NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, record.Position)
	is.NoErr(err)

	// call Read (will return the second record)
	record, err = src.Read(ctx)
	is.NoErr(err)

	is.Equal(record.Position, sdk.Position(fmt.Sprintf(`"%s"`, wants[1].StringType)))
	is.Equal(record.Operation, sdk.OperationCreate)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{keyColumns[0]: wants[1].StringType}))

	got = dataRow{}
	err = json.Unmarshal(record.Payload.After.Bytes(), &got)
	is.NoErr(err)

	is.Equal(got.IntType, wants[1].IntType)
	is.Equal(got.StringType, wants[1].StringType)

	// call Read (will not return the record)
	_, err = src.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	// insert the third records
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (Int32Type,StringType) VALUES (?,?)", cfg[config.Table]),
		wants[2].IntType,
		wants[2].StringType)
	is.NoErr(err)

	// call Read (will return the second record)
	record, err = src.Read(ctx)
	is.NoErr(err)

	is.Equal(record.Position, sdk.Position(fmt.Sprintf(`"%s"`, wants[2].StringType)))
	is.Equal(record.Operation, sdk.OperationCreate)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{keyColumns[0]: wants[2].StringType}))

	got = dataRow{}
	err = json.Unmarshal(record.Payload.After.Bytes(), &got)
	is.NoErr(err)

	is.Equal(got.IntType, wants[2].IntType)
	is.Equal(got.StringType, wants[2].StringType)

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_keyColumns(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = map[string]string{
			config.URL:            getURL(t),
			config.Table:          fmt.Sprintf("CONDUIT_SRC_TEST_%s", randString(6)),
			config.OrderingColumn: "Int32Type2",
			config.KeyColumns:     "Int32Type0",
		}
	)

	db, err := sqlx.Open("clickhouse", cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = db.Ping()
	is.NoErr(err)

	_, err = db.Exec(fmt.Sprintf(`
	CREATE TABLE %s
	(
		Int32Type0	Int32,
		Int32Type1	Int32,
		Int32Type2	Int32,
	) ENGINE ReplacingMergeTree() PRIMARY KEY (Int32Type0, Int32Type1);`, cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		err = dropTable(db, cfg[config.Table])
		is.NoErr(err)
	}()

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (10, 20, 30)", cfg[config.Table]))
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	record, err := src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{"Int32Type0": int32(10)}))

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_keyColumnsPrimaryKeys(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = map[string]string{
			config.URL:            getURL(t),
			config.Table:          fmt.Sprintf("CONDUIT_SRC_TEST_%s", randString(6)),
			config.OrderingColumn: "Int32Type2",
		}
	)

	db, err := sqlx.Open("clickhouse", cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = db.Ping()
	is.NoErr(err)

	_, err = db.Exec(fmt.Sprintf(`
	CREATE TABLE %s
	(
		Int32Type0	Int32,
		Int32Type1	Int32,
		Int32Type2	Int32,
	) ENGINE ReplacingMergeTree() PRIMARY KEY (Int32Type0, Int32Type1);`, cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		err = dropTable(db, cfg[config.Table])
		is.NoErr(err)
	}()

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (10, 20, 30)", cfg[config.Table]))
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	record, err := src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{"Int32Type0": int32(10), "Int32Type1": int32(20)}))

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_keyColumnsOrderingColumn(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = map[string]string{
			config.URL:            getURL(t),
			config.Table:          fmt.Sprintf("CONDUIT_SRC_TEST_%s", randString(6)),
			config.OrderingColumn: "Int32Type1",
		}
	)

	db, err := sqlx.Open("clickhouse", cfg[config.URL])
	is.NoErr(err)
	defer db.Close()

	err = db.Ping()
	is.NoErr(err)

	_, err = db.Exec(fmt.Sprintf(`
	CREATE TABLE %s
	(
		Int32Type0	Int32,
		Int32Type1	Int32,
	) ENGINE Log();`, cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		err = dropTable(db, cfg[config.Table])
		is.NoErr(err)
	}()

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (10, 20)", cfg[config.Table]))
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	record, err := src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{"Int32Type1": int32(20)}))

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func getURL(t *testing.T) string {
	url := os.Getenv("CLICKHOUSE_URL")
	if url == "" {
		t.Skip("CLICKHOUSE_URL env var must be set")

		return ""
	}

	return url
}

func prepareConfig(t *testing.T, keyColumns []string, orderingColumn string) map[string]string {
	return map[string]string{
		config.URL:            getURL(t),
		config.Table:          fmt.Sprintf("CONDUIT_SRC_TEST_%s", randString(6)),
		config.KeyColumns:     strings.Join(keyColumns, ","),
		config.OrderingColumn: orderingColumn,
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

// generates a random string of length n.
func randString(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck // does not actually fail

	return hex.EncodeToString(b)
}
