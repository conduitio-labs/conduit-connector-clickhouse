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

//go:generate paramgen -output=paramgen_src.go SourceConfig

const (
	// OrderingColumn is a config name for an ordering column.
	OrderingColumn = "orderingColumn"
	// Snapshot is the configuration name for the Snapshot field.
	Snapshot = "snapshot"
	// BatchSize is the config name for a batch size.
	BatchSize = "batchSize"
)

// SourceConfig - a source configuration.
type SourceConfig struct {
	Config

	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `json:"orderingColumn" validate:"required"`
	// Snapshot is the configuration that determines whether the connector
	// will take a snapshot of the entire table before starting cdc mode.
	Snapshot bool `default:"true"`
	// BatchSize is a size of rows batch.
	BatchSize int `json:"batchSize" validate:"gt=0,lt=100001" default:"1000"`
}
