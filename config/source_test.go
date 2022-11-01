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

package config

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestParseSource(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want Source
		err  error
	}{
		{
			name: "valid config",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     "id ,name , ,  ,,",
			},
			want: Source{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				OrderingColumn: "id",
				KeyColumns:     []string{"id", "name"},
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "valid config, custom batch size",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     "id,name",
				BatchSize:      "100",
			},
			want: Source{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				KeyColumns:     []string{"id", "name"},
				OrderingColumn: "id",
				BatchSize:      100,
			},
		},
		{
			name: "valid config, batch size is maximum",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "100000",
			},
			want: Source{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				OrderingColumn: "id",
				KeyColumns:     []string{"id"},
				BatchSize:      100000,
			},
		},
		{
			name: "valid config, batch size is minimum",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "1",
			},
			want: Source{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				OrderingColumn: "id",
				KeyColumns:     []string{"id"},
				BatchSize:      1,
			},
		},
		{
			name: "valid config, custom columns",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     "id",
				Columns:        "id ,name ,age ,  ,,",
			},
			want: Source{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				OrderingColumn: "id",
				KeyColumns:     []string{"id"},
				BatchSize:      defaultBatchSize,
				Columns:        []string{"id", "name", "age"},
			},
		},
		{
			name: "invalid config, missed ordering column",
			in: map[string]string{
				URL:        url,
				Table:      table,
				KeyColumns: "id",
				Columns:    "id,name,age",
			},
			err: errors.New("validate config columns: columns must include orderingColumn"),
		},
		{
			name: "invalid config, missed key",
			in: map[string]string{
				URL:            url,
				Table:          table,
				Columns:        "id,name,age",
				OrderingColumn: "id",
			},
			err: errors.New(`validate source config: "keyColumns" value must be set`),
		},
		{
			name: "invalid config, invalid batch size",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "a",
			},
			err: errors.New(`parse BatchSize: strconv.Atoi: parsing "a": invalid syntax`),
		},
		{
			name: "invalid config, missed orderingColumn in columns",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     "name",
				Columns:        "name,age",
			},
			err: fmt.Errorf("validate config columns: %s", errOrderingColumnInclude),
		},
		{
			name: "invalid config, missed keyColumn in columns",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     "name",
				Columns:        "id,age",
			},
			err: fmt.Errorf("validate config columns: %w", errKeyColumnsInclude),
		},
		{
			name: "invalid config, keyColumn is required",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     ",",
				Columns:        "id,age",
			},
			err: fmt.Errorf("validate source config: %w", errRequired(KeyColumns)),
		},
		{
			name: "invalid config, BatchSize is too big",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "100001",
			},
			err: fmt.Errorf("validate source config: %w", errOutOfRange(BatchSize)),
		},
		{
			name: "invalid config, BatchSize is zero",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "0",
			},
			err: fmt.Errorf("validate source config: %w", errOutOfRange(BatchSize)),
		},
		{
			name: "invalid config, BatchSize is negative",
			in: map[string]string{
				URL:            url,
				Table:          table,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "-1",
			},
			err: fmt.Errorf("validate source config: %w", errOutOfRange(BatchSize)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSource(tt.in)
			if err != nil {
				if tt.err == nil {
					t.Errorf("unexpected error: %s", err.Error())

					return
				}

				if err.Error() != tt.err.Error() {
					t.Errorf("unexpected error, got: %s, want: %s", err.Error(), tt.err.Error())

					return
				}

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got: %v, want: %v", got, tt.want)
			}
		})
	}
}
