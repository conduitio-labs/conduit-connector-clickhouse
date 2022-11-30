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
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-clickhouse/config"
	"github.com/conduitio-labs/conduit-connector-clickhouse/destination/mock"
	"github.com/conduitio-labs/conduit-connector-clickhouse/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

const (
	testURL   = "http://username:password@host1:8123/database"
	testTable = "test_table"
)

func TestDestination_Configure_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := NewDestination()

	err := d.Configure(context.Background(), map[string]string{
		config.URL:   testURL,
		config.Table: testTable,
	})
	is.NoErr(err)
}

func TestDestination_Configure_Fail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := NewDestination()

	err := d.Configure(context.Background(), map[string]string{
		config.URL: testURL,
	})
	is.Equal(err.Error(),
		`parse destination config: parse config: validate config: "table" must be set`)
}

func TestDestination_Write_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	metadata := sdk.Metadata{}
	metadata.SetCreatedAt(time.Now())

	records := []sdk.Record{
		{
			Operation: sdk.OperationSnapshot,
			Metadata:  metadata,
			Key: sdk.StructuredData{
				"id": 1,
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"id":   1,
					"name": "John",
				},
			},
		},
		{
			Operation: sdk.OperationCreate,
			Metadata:  metadata,
			Key: sdk.StructuredData{
				"id": 2,
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"id":   2,
					"name": "Sam",
				},
			},
		},
	}

	w := mock.NewMockWriter(ctrl)
	for i := range records {
		w.EXPECT().Insert(ctx, records[i]).Return(nil)
	}

	d := Destination{
		writer: w,
	}

	n, err := d.Write(ctx, records)
	is.NoErr(err)
	is.Equal(n, len(records))
}

func TestDestination_Write_Fail_NoPayload(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	record := sdk.Record{
		Position:  sdk.Position("1.0"),
		Operation: sdk.OperationCreate,
		Key: sdk.StructuredData{
			"id": 1,
		},
	}

	w := mock.NewMockWriter(ctrl)
	w.EXPECT().Insert(ctx, record).Return(writer.ErrNoPayload)

	d := Destination{
		writer: w,
	}

	written, err := d.Write(ctx, []sdk.Record{record})
	is.Equal(err.Error(), `key {"id":1}: no payload`)
	is.Equal(written, 0)
}

func TestDestination_Write_Fail_NoKey(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	record := sdk.Record{
		Position:  sdk.Position("1.0"),
		Operation: sdk.OperationUpdate,
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"id":   1,
				"name": "John",
			},
		},
	}

	w := mock.NewMockWriter(ctrl)
	w.EXPECT().Update(ctx, record).Return(writer.ErrNoKey)

	d := Destination{
		writer: w,
	}

	written, err := d.Write(ctx, []sdk.Record{record})
	is.Equal(err.Error(), "record with no key: key value must be provided")
	is.Equal(written, 0)
}

func TestDestination_Teardown_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	d := Destination{
		writer: mock.NewMockWriter(ctrl),
	}

	err := d.Teardown(ctx)
	is.NoErr(err)
}

func TestDestination_Teardown_Success_NilWriter(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{
		writer: nil,
	}

	err := d.Teardown(context.Background())
	is.NoErr(err)
}
