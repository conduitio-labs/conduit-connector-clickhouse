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

	"github.com/conduitio-labs/conduit-connector-clickhouse/destination/mock"
	"github.com/conduitio-labs/conduit-connector-clickhouse/destination/writer"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestDestination_Write_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	metadata := opencdc.Metadata{}
	metadata.SetCreatedAt(time.Now())

	records := []opencdc.Record{
		{
			Operation: opencdc.OperationSnapshot,
			Metadata:  metadata,
			Key: opencdc.StructuredData{
				"id": 1,
			},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"id":   1,
					"name": "John",
				},
			},
		},
		{
			Operation: opencdc.OperationCreate,
			Metadata:  metadata,
			Key: opencdc.StructuredData{
				"id": 2,
			},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
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

	record := opencdc.Record{
		Position:  opencdc.Position("1.0"),
		Operation: opencdc.OperationCreate,
		Key: opencdc.StructuredData{
			"id": 1,
		},
	}

	w := mock.NewMockWriter(ctrl)
	w.EXPECT().Insert(ctx, record).Return(writer.ErrNoPayload)

	d := Destination{
		writer: w,
	}

	written, err := d.Write(ctx, []opencdc.Record{record})
	is.Equal(err.Error(), `key {"id":1}: no payload`)
	is.Equal(written, 0)
}

func TestDestination_Write_Fail_NoKey(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	record := opencdc.Record{
		Position:  opencdc.Position("1.0"),
		Operation: opencdc.OperationUpdate,
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
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

	written, err := d.Write(ctx, []opencdc.Record{record})
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
