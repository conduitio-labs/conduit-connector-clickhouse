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
	"strings"
)

const (
	// URL is the configuration name of the url.
	URL = "url"
	// Table is the configuration name of the table.
	Table = "table"
	// KeyColumns is the configuration name of key column names (for the Destination),
	// or of the names of the columns to build the record.Key (for the Source), separated by commas.
	KeyColumns = "keyColumns"
)

// Configuration is the configuration needed to connect to ClickHouse database.
type Configuration struct {
	// URL is the configuration of the connection string to connect to ClickHouse database.
	URL string `validate:"required"`
	// Table is the configuration of the table name.
	Table string `validate:"required"`
	// KeyColumns is the configuration of key column names, separated by commas.
	KeyColumns []string
}

// parses a configuration.
func parseConfiguration(cfg map[string]string) (Configuration, error) {
	config := Configuration{
		URL:   strings.TrimSpace(cfg[URL]),
		Table: strings.TrimSpace(cfg[Table]),
	}

	err := validate(config)
	if err != nil {
		return Configuration{}, fmt.Errorf("validate config: %w", err)
	}

	if cfg[KeyColumns] == "" {
		return config, nil
	}

	keyColumns := strings.Split(strings.ReplaceAll(cfg[KeyColumns], " ", ""), ",")
	for i := range keyColumns {
		if keyColumns[i] == "" {
			return Configuration{}, fmt.Errorf("invalid %q", KeyColumns)
		}

		config.KeyColumns = append(config.KeyColumns, keyColumns[i])
	}

	return config, nil
}

// returns a configuration key name by struct field.
func getKeyName(fieldName string) string {
	return map[string]string{
		"URL":            URL,
		"Table":          Table,
		"KeyColumns":     KeyColumns,
		"OrderingColumn": OrderingColumn,
		"Columns":        Columns,
		"BatchSize":      BatchSize,
	}[fieldName]
}
