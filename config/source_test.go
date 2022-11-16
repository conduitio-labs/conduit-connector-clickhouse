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
	"fmt"
	"reflect"
	"testing"
)

func TestParseSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]string
		want Source
		err  error
	}{
		{
			name: "success_required_values",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: testTable,
				},
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_batchSize",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				BatchSize:      "100",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: testTable,
				},
				OrderingColumn: "id",
				BatchSize:      100,
			},
		},
		{
			name: "success_batchSize_max",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				BatchSize:      "100000",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: testTable,
				},
				OrderingColumn: "id",
				BatchSize:      100000,
			},
		},
		{
			name: "success_batchSize_min",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				BatchSize:      "1",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: testTable,
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
		},
		{
			name: "success_columns_has_one_key",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Columns:        "id",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: testTable,
				},
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
				Columns:        []string{"id"},
			},
		},
		{
			name: "success_columns_has_one_key",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Columns:        "id,name",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: testTable,
				},
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
				Columns:        []string{"id", "name"},
			},
		},
		{
			name: "success_columns_space_between_keys",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Columns:        "id, name",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: testTable,
				},
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
				Columns:        []string{"id", "name"},
			},
		},
		{
			name: "success_columns_ends_with_space",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Columns:        "id,name ",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: testTable,
				},
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
				Columns:        []string{"id", "name"},
			},
		},
		{
			name: "success_columns_starts_with_space",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Columns:        " id,name",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: testTable,
				},
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
				Columns:        []string{"id", "name"},
			},
		},
		{
			name: "success_columns_space_between_keys_before_comma",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Columns:        "id ,name",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: testTable,
				},
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
				Columns:        []string{"id", "name"},
			},
		},
		{
			name: "success_columns_two_spaces",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Columns:        "id,  name",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: testTable,
				},
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
				Columns:        []string{"id", "name"},
			},
		},
		{
			name: "failure_required_orderingColumn",
			in: map[string]string{
				URL:     testURL,
				Table:   testTable,
				Columns: "id,name,age",
			},
			err: fmt.Errorf("%q must be set", OrderingColumn),
		},
		{
			name: "failure_invalid_batchSize",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				BatchSize:      "a",
			},
			err: fmt.Errorf(`invalid %q: strconv.Atoi: parsing "a": invalid syntax`, BatchSize),
		},
		{
			name: "failure_missed_orderingColumn_in_columns",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Columns:        "name,age",
			},
			err: fmt.Errorf("columns must include %q", OrderingColumn),
		},
		{
			name: "failure_missed_keyColumn_in_columns",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "name",
				Columns:        "id,age",
			},
			err: fmt.Errorf("columns must include all %q", KeyColumns),
		},
		{
			name: "failure_batchSize_is_too_big",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				BatchSize:      "100001",
			},
			err: fmt.Errorf("%w", fmt.Errorf("%q is out of range", BatchSize)),
		},
		{
			name: "failure_batchSize_is_zero",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				BatchSize:      "0",
			},
			err: fmt.Errorf("%w", fmt.Errorf("%q is out of range", BatchSize)),
		},
		{
			name: "failure_batchSize_is_negative",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				BatchSize:      "-1",
			},
			err: fmt.Errorf("%w", fmt.Errorf("%q is out of range", BatchSize)),
		},
		{
			name: "failure_columns_ends_with_comma",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Columns:        "id,name,",
			},
			err: fmt.Errorf("invalid %q", Columns),
		},
		{
			name: "failure_columns_starts_with_comma",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Columns:        ",id,name",
			},
			err: fmt.Errorf("invalid %q", Columns),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

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
