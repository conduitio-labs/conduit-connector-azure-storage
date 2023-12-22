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
	"time"

	"github.com/miquido/conduit-connector-azure-storage/source/position"
	"github.com/stretchr/testify/require"
)

func TestNewCombinedIterator(t *testing.T) {
	t.Run("Fail to create new iterator with invalid type", func(t *testing.T) {
		iterator, err := NewCombinedIterator(time.Millisecond, nil, "", 1, position.Position{
			Type: 2,
		})

		require.Nil(t, iterator)
		require.EqualError(t, err, "invalid position type (2)")
	})
}

func TestCombinedIterator_Stop(t *testing.T) {
	t.Run("Skips stopping iterator when it is not set", func(t *testing.T) {
		iterator := CombinedIterator{
			iterator: nil,
		}

		require.Nil(t, iterator.iterator)
		iterator.Stop()
		require.Nil(t, iterator.iterator)
	})

	t.Run("Stops iterator when it is set", func(t *testing.T) {
		iteratorMock := IteratorMock{
			StopFunc: func() {},
		}

		iterator := CombinedIterator{
			iterator: &iteratorMock,
		}

		iterator.Stop()
		require.Nil(t, iterator.iterator)
		require.Len(t, iteratorMock.StopCalls(), 1)
	})
}
