// Copyright © 2023 Meroxa, Inc. & Yalantis
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

const (
	// URL is the configuration name of the url.
	URL = "url"
	// Table is the configuration name of the table.
	Table = "table"
	// KeyColumns is the configuration name of key column names (for the DestConfig),
	// or of the names of the columns to build the record.Key (for the SourceConfig), separated by commas.
	KeyColumns = "keyColumns"
)

// Config is the configuration needed to connect to ClickHouse database.
type Config struct {
	// URL is the configuration of the connection string to connect to ClickHouse database.
	URL string `json:"url" validate:"required"`
	// Table is the configuration of the table name.
	Table string `validate:"required"`
	// KeyColumns is the configuration of key column names, separated by commas.
	KeyColumns []string
}
