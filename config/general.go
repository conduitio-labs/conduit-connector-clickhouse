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

package config

import "strings"

const (
	// URL is the configuration name of the url.
	URL = "url"
	// Table is the configuration name of the table.
	Table = "table"
	// PrimaryColumns is the configuration name of columns, separated by commas,
	// with constraints that uniquely identifies each record.
	PrimaryColumns = "primaryColumns"
)

// A General represents a general configuration needed to connect to ClickHouse database.
type General struct {
	// URL is the configuration of the connection string to connect to ClickHouse database.
	URL string `json:"url" validate:"required"`
	// Table is the configuration of the table name.
	Table string `json:"table" validate:"required"`
	// PrimaryColumns is a slice of columns with constraints that uniquely identifies each record.
	PrimaryColumns []string
}

// Parse parses general configuration.
func Parse(cfg map[string]string) (General, error) {
	config := General{
		URL:   strings.TrimSpace(cfg[URL]),
		Table: strings.TrimSpace(cfg[Table]),
	}

	if cfg[PrimaryColumns] != "" {
		primaryColumns := strings.Split(cfg[PrimaryColumns], ",")

		for i := range primaryColumns {
			if strings.TrimSpace(primaryColumns[i]) == "" {
				continue
			}

			config.PrimaryColumns = append(config.PrimaryColumns, strings.TrimSpace(primaryColumns[i]))
		}
	}

	err := validate(config)
	if err != nil {
		return General{}, err
	}

	return config, nil
}

// GetKeyName returns a configuration key name by struct field.
func GetKeyName(fieldName string) string {
	return map[string]string{
		"URL":            URL,
		"Table":          Table,
		"PrimaryColumns": PrimaryColumns,
	}[fieldName]
}
