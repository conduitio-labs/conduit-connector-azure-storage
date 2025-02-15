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

package iterator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jaswdr/faker"
	helper "github.com/miquido/conduit-connector-azure-storage/test"
	"github.com/stretchr/testify/require"
)

func TestCDCIterator(t *testing.T) {
	fakerInstance := faker.New()
	azureBlobClient := helper.NewAzureBlobClient()

	containerName := "cdc-iterator"

	t.Run("Empty container", func(t *testing.T) {
		ctx := context.Background()
		containerClient := helper.PrepareContainer(t, azureBlobClient, containerName)

		iterator, err := NewCDCIterator(time.Millisecond*500, containerClient, time.Now(), fakerInstance.Int32Between(1, 100))
		require.NoError(t, err)

		// Let the Pooling Period pass and iterator to collect blobs
		time.Sleep(time.Second)

		// No blobs were found
		require.False(t, iterator.HasNext(ctx))
	})

	for _, tt := range []struct {
		maxResults int32
	}{
		{
			maxResults: 2,
		},
		{
			maxResults: 3,
		},
	} {
		t.Run(fmt.Sprintf("Reads all 2 blobs when they fit on one page of size %d", tt.maxResults), func(t *testing.T) {
			var (
				record1Name     = fmt.Sprintf("a%s", fakerInstance.File().FilenameWithExtension())
				record1Contents = fakerInstance.Lorem().Sentence(16)
				record2Name     = fmt.Sprintf("b%s", fakerInstance.File().FilenameWithExtension())
				record2Contents = fakerInstance.Lorem().Sentence(16)
			)

			ctx := context.Background()
			containerClient := helper.PrepareContainer(t, azureBlobClient, containerName)

			iterator, err := NewCDCIterator(time.Millisecond*100, containerClient, time.Now().AddDate(0, 0, -1), tt.maxResults)
			require.NoError(t, err)

			require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record1Name, "text/plain", record1Contents))
			require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record2Name, "text/plain", record2Contents))

			// Let the Pooling Period pass and iterator to collect blobs
			time.Sleep(time.Millisecond * 500)

			require.True(t, iterator.HasNext(ctx))
			record1, err := iterator.Next(ctx)
			require.NoError(t, err)
			require.True(t, helper.AssertRecordEquals(t, record1, record1Name, "text/plain", record1Contents))
			require.Equal(t, opencdc.OperationCreate, record1.Operation)

			require.True(t, iterator.HasNext(ctx))
			record2, err := iterator.Next(ctx)
			require.NoError(t, err)
			require.True(t, helper.AssertRecordEquals(t, record2, record2Name, "text/plain", record2Contents))
			require.Equal(t, opencdc.OperationCreate, record2.Operation)

			// Let the Pooling Period pass and iterator to collect blobs
			time.Sleep(time.Millisecond * 500)

			require.False(t, iterator.HasNext(ctx))
			require.Empty(t, iterator.buffer)
		})
	}

	t.Run("Reads all 3 blobs when one page size is 2", func(t *testing.T) {
		var (
			record1Name     = fmt.Sprintf("a%s", fakerInstance.File().FilenameWithExtension())
			record1Contents = fakerInstance.Lorem().Sentence(16)
			record2Name     = fmt.Sprintf("b%s", fakerInstance.File().FilenameWithExtension())
			record2Contents = fakerInstance.Lorem().Sentence(16)
			record3Name     = fmt.Sprintf("c%s", fakerInstance.File().FilenameWithExtension())
			record3Contents = fakerInstance.Lorem().Sentence(16)
		)

		ctx := context.Background()
		containerClient := helper.PrepareContainer(t, azureBlobClient, containerName)

		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record1Name, "text/plain", record1Contents))
		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record2Name, "text/plain", record2Contents))
		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record3Name, "text/plain", record3Contents))

		iterator, err := NewCDCIterator(time.Millisecond*100, containerClient, time.Now().AddDate(0, 0, -1), 2)
		require.NoError(t, err)

		// Let the Pooling Period pass and iterator to collect blobs
		time.Sleep(time.Millisecond * 500)

		require.True(t, iterator.HasNext(ctx))
		record1, err := iterator.Next(ctx)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record1, record1Name, "text/plain", record1Contents))
		require.Equal(t, opencdc.OperationCreate, record1.Operation)

		require.True(t, iterator.HasNext(ctx))
		record2, err := iterator.Next(ctx)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record2, record2Name, "text/plain", record2Contents))
		require.Equal(t, opencdc.OperationCreate, record2.Operation)

		// Let the Pooling Period pass and iterator to collect blobs
		time.Sleep(time.Millisecond * 500)

		require.True(t, iterator.HasNext(ctx))
		record3, err := iterator.Next(ctx)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record3, record3Name, "text/plain", record3Contents))
		require.Equal(t, opencdc.OperationCreate, record3.Operation)

		// Let the Goroutine finish
		time.Sleep(time.Millisecond * 500)

		require.False(t, iterator.HasNext(ctx))
		require.Empty(t, iterator.buffer)
	})

	t.Run("Iterator is stopped while reading", func(t *testing.T) {
		var (
			record1Name     = fmt.Sprintf("a%s", fakerInstance.File().FilenameWithExtension())
			record1Contents = fakerInstance.Lorem().Sentence(16)
			record2Name     = fmt.Sprintf("b%s", fakerInstance.File().FilenameWithExtension())
			record2Contents = fakerInstance.Lorem().Sentence(16)
		)

		ctx := context.Background()
		containerClient := helper.PrepareContainer(t, azureBlobClient, containerName)

		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record1Name, "text/plain", record1Contents))
		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record2Name, "text/plain", record2Contents))

		iterator, err := NewCDCIterator(time.Millisecond*100, containerClient, time.Now().AddDate(0, 0, -1), 100)
		require.NoError(t, err)

		// Let the Goroutine start
		time.Sleep(time.Millisecond * 500)

		require.True(t, iterator.HasNext(ctx))
		record1, err := iterator.Next(ctx)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record1, record1Name, "text/plain", record1Contents))
		require.Equal(t, opencdc.OperationCreate, record1.Operation)

		// Stop the iterator
		iterator.Stop()

		var recordN opencdc.Record
		var errN error

		for {
			recordN, errN = iterator.Next(ctx)
			if errN != nil {
				break
			}
		}

		require.ErrorIs(t, errN, ErrCDCIteratorIsStopped)
		require.Equal(t, opencdc.Record{}, recordN)
	})

	t.Run("Context is cancelled while reading", func(t *testing.T) {
		var (
			record1Name     = fmt.Sprintf("a%s", fakerInstance.File().FilenameWithExtension())
			record1Contents = fakerInstance.Lorem().Sentence(16)
			record2Name     = fmt.Sprintf("b%s", fakerInstance.File().FilenameWithExtension())
			record2Contents = fakerInstance.Lorem().Sentence(16)
		)

		ctx, cancelFunc := context.WithCancel(context.Background())
		containerClient := helper.PrepareContainer(t, azureBlobClient, containerName)

		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record1Name, "text/plain", record1Contents))
		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record2Name, "text/plain", record2Contents))

		iterator, err := NewCDCIterator(time.Millisecond*100, containerClient, time.Now().AddDate(0, 0, -1), 100)
		require.NoError(t, err)

		// Let the Goroutine start
		time.Sleep(time.Millisecond * 500)

		require.True(t, iterator.HasNext(ctx))
		record1, err := iterator.Next(ctx)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record1, record1Name, "text/plain", record1Contents))
		require.Equal(t, opencdc.OperationCreate, record1.Operation)

		// Cancel the context
		cancelFunc()

		var recordN opencdc.Record
		var errN error

		for {
			recordN, errN = iterator.Next(ctx)
			if errN != nil {
				break
			}
		}

		require.EqualError(t, errN, "context canceled")
		require.Equal(t, opencdc.Record{}, recordN)
	})

	t.Run("Reads updated item", func(t *testing.T) {
		var (
			record1Name            = fakerInstance.File().FilenameWithExtension()
			record1Contents        = fmt.Sprintf("a%s", fakerInstance.Lorem().Sentence(16))
			record1ContentsUpdated = fmt.Sprintf("b%s", fakerInstance.Lorem().Sentence(16))
		)

		ctx := context.Background()
		containerClient := helper.PrepareContainer(t, azureBlobClient, containerName)

		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record1Name, "text/plain", record1Contents))

		iterator, err := NewCDCIterator(time.Millisecond*100, containerClient, time.Now().AddDate(0, 0, -1), 100)
		require.NoError(t, err)

		// Let the Goroutine start
		time.Sleep(time.Millisecond * 500)

		require.True(t, iterator.HasNext(ctx))
		record1, err := iterator.Next(ctx)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record1, record1Name, "text/plain", record1Contents))
		require.Equal(t, opencdc.OperationCreate, record1.Operation)

		// Update the blob
		time.Sleep(time.Second)

		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, record1Name, "text/plain", record1ContentsUpdated))

		record2, err := iterator.Next(ctx)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record2, record1Name, "text/plain", record1ContentsUpdated))
		require.Equal(t, opencdc.OperationCreate, record2.Operation)
	})
}
