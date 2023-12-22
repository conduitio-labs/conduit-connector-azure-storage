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

//go:build integration

package iterator

import (
	"context"
	"fmt"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jaswdr/faker"
	"github.com/miquido/conduit-connector-azure-storage/source/position"
	helper "github.com/miquido/conduit-connector-azure-storage/test"
	"github.com/stretchr/testify/require"
)

func TestCombinedIterator(t *testing.T) {
	ctx := context.Background()
	fakerInstance := faker.New()
	azureBlobClient := helper.NewAzureBlobClient()

	containerName := "combined-iterator"

	t.Run("Empty container", func(t *testing.T) {
		helper.PrepareContainer(t, azureBlobClient, containerName)

		iterator, err := NewCombinedIterator(time.Millisecond*500, azureBlobClient, containerName, fakerInstance.Int32Between(1, 100), position.NewDefaultSnapshotPosition())
		require.NoError(t, err)

		// Let the Goroutine finish
		time.Sleep(time.Second)

		require.IsType(t, &SnapshotIterator{}, iterator.iterator)

		// No blobs were found
		require.False(t, iterator.HasNext(ctx))

		// Iterators were swapped
		require.IsType(t, &CDCIterator{}, iterator.iterator)
	})

	t.Run("When snapshot iterator finishes, iterator is switched to CDC", func(t *testing.T) {
		var (
			record1Name     = fmt.Sprintf("a%s", fakerInstance.File().FilenameWithExtension())
			record1Contents = fakerInstance.Lorem().Sentence(16)
			record2Name     = fmt.Sprintf("b%s", fakerInstance.File().FilenameWithExtension())
			record2Contents = fakerInstance.Lorem().Sentence(16)
			record3Name     = fmt.Sprintf("c%s", fakerInstance.File().FilenameWithExtension())
			record3Contents = fakerInstance.Lorem().Sentence(16)
		)

		helper.PrepareContainer(t, azureBlobClient, containerName)
		snapshotPosition := position.NewDefaultSnapshotPosition()

		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record1Name, "text/plain", record1Contents))
		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record2Name, "text/plain", record2Contents))

		iterator, err := NewCombinedIterator(time.Millisecond*100, azureBlobClient, containerName, 100, snapshotPosition)
		require.NoError(t, err)

		// Let the Goroutine run
		time.Sleep(time.Second)

		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record3Name, "text/plain", record3Contents))

		require.True(t, iterator.HasNext(ctx))
		record1, err := iterator.Next(ctx)
		require.IsType(t, &SnapshotIterator{}, iterator.iterator)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record1, record1Name, "text/plain", record1Contents))
		require.Equal(t, sdk.OperationSnapshot, record1.Operation)

		require.True(t, iterator.HasNext(ctx))
		record2, err := iterator.Next(ctx)
		require.IsType(t, &SnapshotIterator{}, iterator.iterator)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record2, record2Name, "text/plain", record2Contents))
		require.Equal(t, sdk.OperationSnapshot, record2.Operation)

		// Let the Goroutine finish
		time.Sleep(time.Millisecond * 500)

		// Iterators were swapped
		require.False(t, iterator.HasNext(ctx))
		require.IsType(t, &CDCIterator{}, iterator.iterator)

		// Let the Pooling Period pass and iterator to collect blobs
		time.Sleep(time.Millisecond * 500)

		require.True(t, iterator.HasNext(ctx))
		record3, err := iterator.Next(ctx)
		require.IsType(t, &CDCIterator{}, iterator.iterator)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record3, record3Name, "text/plain", record3Contents))
		require.Equal(t, sdk.OperationCreate, record3.Operation)

		require.False(t, iterator.HasNext(ctx))
		require.IsType(t, &CDCIterator{}, iterator.iterator)
	})
}
