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

package iterator

import (
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Position represents ClickHouse's position.
type Position struct {
	// LastProcessedValue represents the last processed value from ordering column.
	LastProcessedValue any `json:"lastProcessedValue"`
	// LatestSnapshotValue represents the most recent value of ordering column.
	LatestSnapshotValue any `json:"latestSnapshotValue"`
}

// ParseSDKPosition parses sdk.Position and returns Position.
func ParseSDKPosition(position sdk.Position) (*Position, error) {
	var pos Position

	if position == nil {
		return &pos, nil
	}

	if err := json.Unmarshal(position, &pos); err != nil {
		return nil, fmt.Errorf("unmarshal sdk.Position into Position: %w", err)
	}

	return &pos, nil
}

// marshal marshals Position and returns sdk.Position or an error.
func (p Position) marshal() (sdk.Position, error) {
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return positionBytes, nil
}
