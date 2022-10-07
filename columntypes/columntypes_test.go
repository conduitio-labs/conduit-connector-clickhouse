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

package columntypes

import (
	"reflect"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestConvertStructureData(t *testing.T) {
	t.Parallel()

	columnTypes := map[string]string{
		"int_field":             "int",
		"date_field":            chTypeDate,
		"datetime_field":        chTypeDateTime,
		"datetime_string_field": chTypeDateTime,
	}
	data := sdk.StructuredData{
		"int_field":             42,
		"date_field":            time.Unix(957123851, 0).UTC(),
		"datetime_field":        time.Unix(957123851, 0).UTC(),
		"datetime_string_field": "2000-04-30T19:44:11+00:00",
	}
	want := sdk.StructuredData{
		"int_field":             42,
		"date_field":            "2000-04-30 19:44:11",
		"datetime_field":        "2000-04-30 19:44:11",
		"datetime_string_field": "2000-04-30 19:44:11",
	}

	got, err := ConvertStructureData(columnTypes, data)
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())

		return
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}
}
