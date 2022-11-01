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

func TestDestination_Write(t *testing.T) {
	type dataRow struct {
		intType         int
		stringType      string
		floatType       float32
		doubleType      float64
		booleanType     bool
		uuidType        uuid.UUID
		dateType        time.Time
		datetimeType    time.Time
		arrayIntType    []int32
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
		arrayIntType:    []int32{10, 20, 30},
		arrayStringType: []string{"test_a", "test_b", "test_c"},
		mapType: map[string]int32{
			"test1": 1,
			"test2": 2,
		},
	}

	record := sdk.Record{
		Operation: sdk.OperationSnapshot,
		Payload: sdk.Change{After: sdk.StructuredData{
			"int_type":          want.intType,
			"string_type":       want.stringType,
			"float_type":        want.floatType,
			"double_type":       want.doubleType,
			"boolean_type":      want.booleanType,
			"uuid_type":         want.uuidType,
			"date_type":         want.dateType,
			"datetime_type":     want.datetimeType,
			"array_int_type":    want.arrayIntType,
			"array_string_type": want.arrayStringType,
			"map_type":          want.mapType,
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
	err = db.QueryRow(fmt.Sprintf("SELECT * FROM %s WHERE int_type = %d;", cfg[config.Table], 42)).
		Scan(&res.intType,
			&res.stringType,
			&res.floatType,
			&res.doubleType,
			&res.booleanType,
			&res.uuidType,
			&res.dateType,
			&res.datetimeType,
			&res.arrayIntType,
			&res.arrayStringType,
			&res.mapType)
	is.NoErr(err)

	// time with the location set to UTC
	res.dateType = res.dateType.UTC()
	res.datetimeType = res.datetimeType.UTC()
	is.Equal(res, want)
}

func TestDestination_Write_Update(t *testing.T) {
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
	cfg[config.KeyColumns] = "int_type"

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	n, err := dest.Write(ctx, []sdk.Record{
		{
			Operation: sdk.OperationUpdate,
			Key: sdk.StructuredData{
				"int_type": 42,
			},
			Payload: sdk.Change{After: sdk.StructuredData{
				"int_type":    42,
				"string_type": "Jane",
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
				"int_type":    42,
				"string_type": "Sam",
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

func TestDestination_Write_Delete(t *testing.T) {
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
			Key:       sdk.RawData(`{"int_type":42}`),
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

func TestDestination_Write_WrongColumn(t *testing.T) {
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
				"int_type":     43,
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
    int_type          Int32,
    string_type       String,
    float_type        Float32,
	double_type       Float64,
	boolean_type      Bool,
	uuid_type         UUID,
	date_type         Date,
	datetime_type     DateTime,
	array_int_type    Array(Int32),
	array_string_type Array(String),
    map_type          Map(String, Int32)
) ENGINE ReplacingMergeTree() PRIMARY KEY int_type;`, table))
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
	_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (int_type, string_type) VALUES (42, 'Sam');", table))
	if err != nil {
		return fmt.Errorf("execute insert query: %w", err)
	}

	return nil
}

func getStringFieldByIntField(db *sqlx.DB, table string, id int) (string, error) {
	// wait a bit
	time.Sleep(time.Second)

	row := db.QueryRow(fmt.Sprintf("SELECT string_type FROM %s WHERE int_type = %d;", table, id))

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
