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

func TestParseDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]string
		want Destination
		err  error
	}{
		{
			name: "success_required_values",
			in: map[string]string{
				URL:   url,
				Table: table,
			},
			want: Destination{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
			},
		},
		{
			name: "success_keyColumns_has_one_key",
			in: map[string]string{
				URL:        url,
				Table:      table,
				KeyColumns: "id",
			},
			want: Destination{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				KeyColumns: []string{"id"},
			},
		},
		{
			name: "success_keyColumns_has_two_keys",
			in: map[string]string{
				URL:        url,
				Table:      table,
				KeyColumns: "id,name",
			},
			want: Destination{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "success_keyColumns_space_between_keys",
			in: map[string]string{
				URL:        url,
				Table:      table,
				KeyColumns: "id, name",
			},
			want: Destination{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "success_keyColumns_ends_with_space",
			in: map[string]string{
				URL:        url,
				Table:      table,
				KeyColumns: "id,name ",
			},
			want: Destination{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "success_keyColumns_starts_with_space",
			in: map[string]string{
				URL:        url,
				Table:      table,
				KeyColumns: " id,name",
			},
			want: Destination{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "success_keyColumns_space_between_keys_before_comma",
			in: map[string]string{
				URL:        url,
				Table:      table,
				KeyColumns: "id ,name",
			},
			want: Destination{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "success_keyColumns_two_spaces",
			in: map[string]string{
				URL:        url,
				Table:      table,
				KeyColumns: "id,  name",
			},
			want: Destination{
				Configuration: Configuration{
					URL:   url,
					Table: table,
				},
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "failure_keyColumns_ends_with_comma",
			in: map[string]string{
				URL:        url,
				Table:      table,
				KeyColumns: "id,name,",
			},
			err: fmt.Errorf("invalid %q", KeyColumns),
		},
		{
			name: "failure_keyColumns_starts_with_comma",
			in: map[string]string{
				URL:        url,
				Table:      table,
				KeyColumns: ",id,name",
			},
			err: fmt.Errorf("invalid %q", KeyColumns),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseDestination(tt.in)
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
