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
			name: "valid config with keyColumns field",
			in: map[string]string{
				URL:        "http://username:password@host1:8123/database",
				Table:      "test_table",
				KeyColumns: "id",
			},
			want: Destination{
				General: General{
					URL:   "http://username:password@host1:8123/database",
					Table: "test_table",
				},
				KeyColumns: []string{"id"},
			},
		},
		{
			name: "valid config with keyColumns fields",
			in: map[string]string{
				URL:        "http://username:password@host1:8123/database",
				Table:      "test_table",
				KeyColumns: "id ,name , ,  ,,",
			},
			want: Destination{
				General: General{
					URL:   "http://username:password@host1:8123/database",
					Table: "test_table",
				},
				KeyColumns: []string{"id", "name"},
			},
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
