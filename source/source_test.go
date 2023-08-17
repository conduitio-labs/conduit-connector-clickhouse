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
	"errors"
	"testing"

	"github.com/conduitio-labs/conduit-connector-clickhouse/source/mock"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

func TestSource_Read(t *testing.T) {
	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	st := make(sdk.StructuredData)
	st["key"] = "value"

	record := sdk.Record{
		Position: sdk.Position(`{"last_processed_element_value": 1}`),
		Metadata: nil,
		Key:      st,
		Payload:  sdk.Change{After: st},
	}

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next(ctx).Return(record, nil)

	s := Source{
		iterator: it,
	}

	r, err := s.Read(ctx)
	is.NoErr(err)

	is.Equal(r, record)
}

func TestSource_Read_hasNextFail(t *testing.T) {
	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, errors.New("get data: fail"))

	s := Source{
		iterator: it,
	}

	_, err := s.Read(ctx)
	is.Equal(err.Error(), "has next: get data: fail")
}

func TestSource_Read_nextFail(t *testing.T) {
	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next(ctx).Return(sdk.Record{}, errors.New("key is not exist"))

	s := Source{
		iterator: it,
	}

	_, err := s.Read(ctx)
	is.Equal(err.Error(), "next: key is not exist")
}

func TestSource_Teardown(t *testing.T) {
	is := is.New(t)

	ctrl := gomock.NewController(t)

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Stop().Return(nil)

	s := Source{
		iterator: it,
	}

	err := s.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Teardown_fail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Stop().Return(errors.New("some error"))

	s := Source{
		iterator: it,
	}

	err := s.Teardown(context.Background())
	is.Equal(err.Error(), "stop iterator: some error")
}
