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
	"testing"

	"github.com/conduitio-labs/conduit-connector-clickhouse/config"
	"github.com/matryer/is"
)

const (
	testURL   = "http://username:password@host1:8123/database"
	testTable = "test_table"
)

func TestSource_Configure_success(t *testing.T) {
	is := is.New(t)

	s := Source{}

	err := s.Configure(context.Background(), map[string]string{
		config.URL:            testURL,
		config.Table:          testTable,
		config.KeyColumns:     "id",
		config.OrderingColumn: "created_at",
	})
	is.NoErr(err)
	is.Equal(s.config, config.Source{
		Configuration: config.Configuration{
			URL:   testURL,
			Table: testTable,
		},
		KeyColumns:     []string{"id"},
		OrderingColumn: "created_at",
		BatchSize:      1000,
	})
}

func TestSource_Configure_fail(t *testing.T) {
	is := is.New(t)

	s := Source{}

	err := s.Configure(context.Background(), map[string]string{
		config.URL:        testURL,
		config.Table:      testTable,
		config.KeyColumns: "id",
	})
	is.Equal(err.Error(), `parse source config: "orderingColumn" must be set`)
}
