// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package config

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	SourceConfigBatchSize      = "batchSize"
	SourceConfigKeyColumns     = "keyColumns"
	SourceConfigOrderingColumn = "orderingColumn"
	SourceConfigSnapshot       = "snapshot"
	SourceConfigTable          = "table"
	SourceConfigUrl            = "url"
)

func (SourceConfig) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		SourceConfigBatchSize: {
			Default:     "1000",
			Description: "BatchSize is a size of rows batch.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{
				config.ValidationGreaterThan{V: 0},
				config.ValidationLessThan{V: 100001},
			},
		},
		SourceConfigKeyColumns: {
			Default:     "",
			Description: "KeyColumns is the configuration of key column names, separated by commas.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		SourceConfigOrderingColumn: {
			Default:     "",
			Description: "OrderingColumn is a name of a column that the connector will use for ordering rows.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		SourceConfigSnapshot: {
			Default:     "true",
			Description: "Snapshot is the configuration that determines whether the connector\nwill take a snapshot of the entire table before starting cdc mode.",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
		SourceConfigTable: {
			Default:     "",
			Description: "Table is the configuration of the table name.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		SourceConfigUrl: {
			Default:     "",
			Description: "URL is the configuration of the connection string to connect to ClickHouse database.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}
