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
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-clickhouse/config"
	"github.com/conduitio-labs/conduit-connector-clickhouse/destination/mock"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

func TestDestination_ConfigureSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{}

	err := d.Configure(context.Background(), map[string]string{
		config.URL:            "localhost:8123?username=user&password=password&database=default",
		config.Table:          "test_table",
		config.PrimaryColumns: "id",
	})
	is.NoErr(err)
	is.Equal(d.cfg, config.General{
		URL:            "localhost:8123?username=user&password=password&database=default",
		Table:          "test_table",
		PrimaryColumns: []string{"id"},
	})
}

func TestDestination_ConfigureFail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{}

	err := d.Configure(context.Background(), map[string]string{
		config.URL: "localhost:8123?username=user&password=password&database=default",
	})
	is.True(err != nil)
}

func TestDestination_WriteSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	metadata := sdk.Metadata{}
	metadata.SetCreatedAt(time.Now())

	records := make([]sdk.Record, 2)
	records[0] = sdk.Record{
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
	}
	records[1] = sdk.Record{
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
	}

	w := mock.NewMockWriter(ctrl)
	for i := range records {
		w.EXPECT().Write(ctx, records[i]).Return(nil)
	}

	d := Destination{
		writer: w,
	}

	n, err := d.Write(ctx, records)
	is.NoErr(err)
	is.Equal(n, len(records))
}

func TestDestination_TeardownSuccess(t *testing.T) {
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

func TestDestination_TeardownSuccessNilWriter(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{
		writer: nil,
	}

	err := d.Teardown(context.Background())
	is.NoErr(err)
}
