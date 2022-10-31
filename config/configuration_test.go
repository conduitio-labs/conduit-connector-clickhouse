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

	"go.uber.org/multierr"
)

const (
	url   = "http://username:password@host1:8123/database"
	table = "test_table"
)

func TestParseGeneral(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]string
		want configuration
		err  error
	}{
		{
			name: "valid config",
			in: map[string]string{
				URL:   url,
				Table: table,
			},
			want: configuration{
				URL:   url,
				Table: table,
			},
		},
		{
			name: "url is required",
			in: map[string]string{
				Table: table,
			},
			err: fmt.Errorf("validate general config: %w", errRequired(URL)),
		},
		{
			name: "table is required",
			in: map[string]string{
				URL: url,
			},
			err: fmt.Errorf("validate general config: %w", errRequired(Table)),
		},
		{
			name: "a couple required fields are empty (a password and an url)",
			in:   map[string]string{},
			err:  fmt.Errorf("validate general config: %w", multierr.Combine(errRequired(URL), errRequired(Table))),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseGeneral(tt.in)
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
