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

package iterator

import (
	"errors"
	"reflect"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
)

func TestParseSDKPosition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   opencdc.Position
		want Position
		err  error
	}{
		{
			name: "success_position_is_nil",
			in:   nil,
			want: Position{},
		},
		{
			name: "success_float64_fields",
			in: opencdc.Position(`{
			   "lastProcessedValue":10,
			   "latestSnapshotValue":30
			}`),
			want: Position{
				LastProcessedValue:  float64(10),
				LatestSnapshotValue: float64(30),
			},
		},
		{
			name: "success_string_fields",
			in: opencdc.Position(`{
			   "lastProcessedValue":"abc",
			   "latestSnapshotValue":"def"
			}`),
			want: Position{
				LastProcessedValue:  "abc",
				LatestSnapshotValue: "def",
			},
		},
		{
			name: "success_lastProcessedValue_only",
			in: opencdc.Position(`{
			   "lastProcessedValue":10
			}`),
			want: Position{
				LastProcessedValue: float64(10),
			},
		},
		{
			name: "success_latestSnapshotValue_only",
			in: opencdc.Position(`{
			   "latestSnapshotValue":30
			}`),
			want: Position{
				LatestSnapshotValue: float64(30),
			},
		},
		{
			name: "failure_required_url_and_table",
			in:   opencdc.Position("invalid"),
			err: errors.New("unmarshal opencdc.Position into Position: " +
				"invalid character 'i' looking for beginning of value"),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseSDKPosition(tt.in)
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

			if !reflect.DeepEqual(*got, tt.want) {
				t.Errorf("got: %v, want: %v", *got, tt.want)
			}
		})
	}
}

func TestMarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   Position
		want opencdc.Position
	}{
		{
			name: "success_position_is_nil",
			in:   Position{},
			want: opencdc.Position(`{"lastProcessedValue":null,"latestSnapshotValue":null}`),
		},
		{
			name: "success_integer_fields",
			in: Position{
				LastProcessedValue:  10,
				LatestSnapshotValue: 30,
			},
			want: opencdc.Position(`{"lastProcessedValue":10,"latestSnapshotValue":30}`),
		},
		{
			name: "success_string_fields",
			in: Position{
				LastProcessedValue:  "abc",
				LatestSnapshotValue: "def",
			},
			want: opencdc.Position(`{"lastProcessedValue":"abc","latestSnapshotValue":"def"}`),
		},
		{
			name: "success_lastProcessedValue_only",
			in: Position{
				LastProcessedValue: float64(10),
			},
			want: opencdc.Position(`{"lastProcessedValue":10,"latestSnapshotValue":null}`),
		},
		{
			name: "success_latestSnapshotValue_only",
			in: Position{
				LatestSnapshotValue: float64(30),
			},
			want: opencdc.Position(`{"lastProcessedValue":null,"latestSnapshotValue":30}`),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.in.marshal()
			if err != nil {
				t.Errorf("unexpected error: %s", err.Error())

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got: %v, want: %v", string(got), string(tt.want))
			}
		})
	}
}
