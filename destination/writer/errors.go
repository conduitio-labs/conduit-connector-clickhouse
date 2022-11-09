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

package writer

import "errors"

var (
	// ErrNoPayload occurs when there's no payload to insert or update.
	ErrNoPayload = errors.New("no payload")
	// ErrNoKey occurs when there is no value for key.
	ErrNoKey = errors.New("key value must be provided")
)
