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
	"strings"
)

const (
	// OrderingColumn is a config name for an ordering column.
	OrderingColumn = "orderingColumn"
	// Columns is the config name for a list of columns, separated by commas.
	Columns = "columns"
	// BatchSize is the config name for a batch size.
	BatchSize = "batchSize"

	defaultBatchSize = 1000
)

// A Source represents a source configuration.
type Source struct {
	General

	// KeyColumns is the configuration of key column names, separated by commas.
	KeyColumns []string `validate:"required"`
	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `validate:"required"`
	// Columns list of column names that should be included in each Record's payload.
	Columns []string
	// BatchSize is a size of rows batch.
	BatchSize int `validate:"gte=1,lte=100000"`
}

// ParseSource parses source configuration.
func ParseSource(cfg map[string]string) (Source, error) {
	config, err := parseGeneral(cfg)
	if err != nil {
		return Source{}, fmt.Errorf("parse general config: %w", err)
	}

	sourceConfig := Source{
		General:        config,
		OrderingColumn: cfg[OrderingColumn],
		BatchSize:      defaultBatchSize,
	}

	keyColumns := strings.Split(cfg[KeyColumns], ",")
	for i := range keyColumns {
		if keyColumn := strings.TrimSpace(keyColumns[i]); keyColumn != "" {
			sourceConfig.KeyColumns = append(sourceConfig.KeyColumns, keyColumn)
		}
	}

	if cfg[Columns] != "" {
		columnsSl := strings.Split(cfg[Columns], ",")
		for i := range columnsSl {
			if column := strings.TrimSpace(columnsSl[i]); column != "" {
				sourceConfig.Columns = append(sourceConfig.Columns, column)
			}
		}

		err = validateColumns(sourceConfig.OrderingColumn, sourceConfig.KeyColumns, sourceConfig.Columns)
		if err != nil {
			return Source{}, fmt.Errorf("validate config columns: %w", err)
		}
	}

	if cfg[BatchSize] != "" {
		sourceConfig.BatchSize, err = strconv.Atoi(cfg[BatchSize])
		if err != nil {
			return Source{}, fmt.Errorf("parse BatchSize: %w", err)
		}
	}

	err = validate(sourceConfig)
	if err != nil {
		return Source{}, fmt.Errorf("validate source config: %w", err)
	}

	return sourceConfig, nil
}
