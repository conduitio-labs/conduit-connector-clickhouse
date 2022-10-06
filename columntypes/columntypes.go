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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-clickhouse/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// ClickHouse data types.
	chTypeDate     = "Date"
	chTypeDateTime = "DateTime"

	// column names.
	colName = "name"
	colType = "type"

	// datetime layout.
	layoutDateTime = "2006-01-02 15:04:05"

	// query pattern that selects the name and type of the columns by table name.
	queryDescribeTable = "DESCRIBE TABLE %s"
)

var timeLayouts = []string{time.RFC3339, time.RFC3339Nano, time.Layout, time.ANSIC, time.UnixDate, time.RubyDate,
	time.RFC822, time.RFC822Z, time.RFC850, time.RFC1123, time.RFC1123Z, time.RFC3339, time.RFC3339,
	time.RFC3339Nano, time.Kitchen, time.Stamp, time.StampMilli, time.StampMicro, time.StampNano}

// GetColumnTypes returns a map containing the names and types of the table columns.
func GetColumnTypes(ctx context.Context, repo *repository.ClickHouse, tableName string) (map[string]string, error) {
	dest := make(map[string]any)

	rows, err := repo.DB.QueryxContext(ctx, fmt.Sprintf(queryDescribeTable, tableName))
	if err != nil {
		return nil, fmt.Errorf("query column types: %w", err)
	}

	columnTypes := make(map[string]string)
	for rows.Next() {
		if err = rows.MapScan(dest); err != nil {
			return nil, fmt.Errorf("map scan: %w", err)
		}

		columnTypes[dest[colName].(string)] = dest[colType].(string)
	}

	return columnTypes, nil
}

// ConvertStructureData converts a sdk.StructureData values to a proper database types.
func ConvertStructureData(
	columnTypes map[string]string,
	data sdk.StructuredData,
) (sdk.StructuredData, error) {
	result := make(sdk.StructuredData, len(data))

	for key, value := range data {
		if value == nil {
			result[key] = nil

			continue
		}

		switch t := columnTypes[key]; {
		case strings.Contains(t, chTypeDate), strings.Contains(t, chTypeDateTime):
			timeStr, err := formatDatetime(value)
			if err != nil {
				return nil, fmt.Errorf("format datetime value: %w", err)
			}

			result[key] = timeStr
		default:
			result[key] = value
		}
	}

	return result, nil
}

func formatDatetime(value any) (string, error) {
	timeValue, ok := value.(time.Time)
	if ok {
		return timeValue.Format(layoutDateTime), nil
	}

	valueStr, ok := value.(string)
	if !ok {
		return "", errValueIsNotAString
	}

	timeValue, err := parseTime(valueStr)
	if err != nil {
		return "", fmt.Errorf("convert value to time.Time: %w", err)
	}

	return timeValue.Format(layoutDateTime), nil
}

func parseTime(val string) (time.Time, error) {
	for i := range timeLayouts {
		timeValue, err := time.Parse(timeLayouts[i], val)
		if err != nil {
			continue
		}

		return timeValue, nil
	}

	return time.Time{}, fmt.Errorf("%s - %w", val, errInvalidTimeLayout)
}
