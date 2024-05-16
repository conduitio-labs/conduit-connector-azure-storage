// Copyright © 2022 Meroxa, Inc. and Miquido
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

//go:generate moq -out iterator_moq_test.go . Iterator

package iterator

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Iterator interface {
	// HasNext indicates whether there is new sdk.Record available (`true`) or not (`false`)
	HasNext(ctx context.Context) bool

	// Next returns new sdk.Record while reading the container or error when operation failed
	Next(ctx context.Context) (sdk.Record, error)

	// Stop informs the iterator to stop processing new records.
	// All currently ongoing operations should be gracefully shut down.
	Stop()
}
