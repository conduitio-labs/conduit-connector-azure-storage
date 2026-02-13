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

//go:build unit

package iterator

import (
	"testing"

	"github.com/conduitio-labs/conduit-connector-azure-storage/source/position"
	"github.com/stretchr/testify/require"
)

func TestNewSnapshotIterator(t *testing.T) {
	t.Run("Fail to create iterator with Max Results less than 1", func(t *testing.T) {
		iterator, err := NewSnapshotIterator(nil, position.Position{}, 0)
		require.Nil(t, iterator)
		require.EqualError(t, err, "maxResults is expected to be greater than or equal to 1, got 0")
	})
}
