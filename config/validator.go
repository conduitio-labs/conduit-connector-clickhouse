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
	"errors"
	"fmt"
	"sync"

	v "github.com/go-playground/validator/v10"
	"go.uber.org/multierr"
)

const (
	requiredErrMsg   = "%q value must be set"
	outOfRangeErrMsg = "%q is out of range"
)

var (
	validatorInstance *v.Validate
	once              sync.Once

	errKeyColumnsInclude     = errors.New("columns must include all keyColumns")
	errOrderingColumnInclude = errors.New("columns must include orderingColumn")
)

// Get initializes and registers validation tags once, and returns validator instance.
func Get() *v.Validate {
	once.Do(func() {
		validatorInstance = v.New()
	})

	return validatorInstance
}

// validate validates structs.
func validate(s interface{}) error {
	var err error

	validationErr := Get().Struct(s)
	if validationErr != nil {
		if _, ok := validationErr.(*v.InvalidValidationError); ok {
			return fmt.Errorf("validate general config struct: %w", validationErr)
		}

		for _, e := range validationErr.(v.ValidationErrors) {
			switch e.ActualTag() {
			case "required":
				err = multierr.Append(err, errRequired(getKeyName(e.Field())))
			case "gte", "lte":
				err = multierr.Append(err, errOutOfRange(getKeyName(e.Field())))
			}
		}
	}

	return err
}

// checks that the Columns configuration field with a list of columns,
// contains an orderingColumn and all keyColumns.
func validateColumns(orderingColumn string, keyColumns, columns []string) error {
	if orderingColumn == "" && len(keyColumns) == 0 {
		return nil
	}

	columnsMap := make(map[string]struct{}, len(columns))
	for i := 0; i < len(columns); i++ {
		columnsMap[columns[i]] = struct{}{}
	}

	if _, ok := columnsMap[orderingColumn]; !ok {
		return errOrderingColumnInclude
	}

	for i := range keyColumns {
		if _, ok := columnsMap[keyColumns[i]]; !ok {
			return errKeyColumnsInclude
		}
	}

	return nil
}

// returns the formatted required field error.
func errRequired(name string) error {
	return fmt.Errorf(requiredErrMsg, name)
}

// returns the formatted out of range error.
func errOutOfRange(name string) error {
	return fmt.Errorf(outOfRangeErrMsg, name)
}
