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
	"strconv"
)

const (
	// OrderingColumn is a config name for an ordering column.
	OrderingColumn = "orderingColumn"
	// Snapshot is the configuration name for the Snapshot field.
	Snapshot = "snapshot"
	// BatchSize is the config name for a batch size.
	BatchSize = "batchSize"

	// defaultBatchSize is the default value of the BatchSize field.
	defaultBatchSize = 1000
	// defaultSnapshot is a default value for the Snapshot field.
	defaultSnapshot = true
)

// Source - a source configuration.
type Source struct {
	Configuration

	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `validate:"required"`
	// Snapshot is the configuration that determines whether the connector
	// will take a snapshot of the entire table before starting cdc mode.
	Snapshot bool
	// BatchSize is a size of rows batch.
	BatchSize int `validate:"gte=1,lte=100000"`
}

// ParseSource parses source configuration.
func ParseSource(cfg map[string]string) (Source, error) {
	config, err := parseConfiguration(cfg)
	if err != nil {
		return Source{}, fmt.Errorf("parse source config: %w", err)
	}

	sourceConfig := Source{
		Configuration:  config,
		OrderingColumn: cfg[OrderingColumn],
		Snapshot:       defaultSnapshot,
		BatchSize:      defaultBatchSize,
	}

	if cfg[Snapshot] != "" {
		snapshot, err := strconv.ParseBool(cfg[Snapshot])
		if err != nil {
			return Source{}, fmt.Errorf("parse %q: %w", Snapshot, err)
		}

		sourceConfig.Snapshot = snapshot
	}

	if cfg[BatchSize] != "" {
		sourceConfig.BatchSize, err = strconv.Atoi(cfg[BatchSize])
		if err != nil {
			return Source{}, fmt.Errorf("invalid %q: %w", BatchSize, err)
		}
	}

	err = validate(sourceConfig)
	if err != nil {
		return Source{}, err
	}

	return sourceConfig, nil
}
