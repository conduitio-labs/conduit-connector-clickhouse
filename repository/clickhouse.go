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

package repository

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	// Go driver for ClickHouse.
	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// ClickHouse represents a ClickHouse repository.
type ClickHouse struct {
	DB *sqlx.DB
}

// New opens a database and pings it.
func New(url string) (*ClickHouse, error) {
	db, err := sqlx.Open("clickhouse", url)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &ClickHouse{DB: db}, nil
}

// Close closes database.
func (ch *ClickHouse) Close() error {
	if ch != nil {
		return ch.DB.Close()
	}

	return nil
}
