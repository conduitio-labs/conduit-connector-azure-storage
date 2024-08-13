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

package source

import (
	"context"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jaswdr/faker"
	"github.com/miquido/conduit-connector-azure-storage/source/position"
	helper "github.com/miquido/conduit-connector-azure-storage/test"
	"github.com/stretchr/testify/require"
)

func TestSource_FailsWhenConnectionStringIsInvalid(t *testing.T) {
	ctx := context.Background()

	cfgRaw := map[string]string{
		ConfigKeyConnectionString: "invalid connection string",
		ConfigKeyContainerName:    "source-integration-tests",
		ConfigKeyPollingPeriod:    "1500ms",
		ConfigKeyMaxResults:       "1",
	}

	src := NewSource()

	require.NoError(t, src.Configure(ctx, cfgRaw))

	t.Cleanup(func() {
		_ = src.Teardown(ctx)
	})

	require.ErrorContains(t, src.Open(ctx, nil), "connector open error: could not create account connection client: connection string is either blank or malformed.")
}

func TestSource_FailsWhenConnectionCannotBeEstablished(t *testing.T) {
	ctx := context.Background()

	cfgRaw := map[string]string{
		ConfigKeyConnectionString: "DefaultEndpointsProtocol=http;AccountName=account;AccountKey=QWNjb3VudEtleQ==;BlobEndpoint=http://127.0.0.1:10101/account",
		ConfigKeyContainerName:    "source-integration-tests",
		ConfigKeyPollingPeriod:    "1500ms",
		ConfigKeyMaxResults:       "1",
	}

	src := NewSource()

	require.NoError(t, src.Configure(ctx, cfgRaw))

	t.Cleanup(func() {
		_ = src.Teardown(ctx)
	})

	require.ErrorContains(t, src.Open(ctx, nil), "connector open error: could not establish a connection")
}

func TestSource_FailsWhenContainerDoesNotExist(t *testing.T) {
	ctx := context.Background()

	var (
		containerName = "source-integration-tests"

		cfgRaw = map[string]string{
			ConfigKeyConnectionString: helper.GetConnectionString(),
			ConfigKeyContainerName:    containerName,
			ConfigKeyPollingPeriod:    "1500ms",
			ConfigKeyMaxResults:       "1",
		}
	)

	src := NewSource()

	require.NoError(t, src.Configure(ctx, cfgRaw))

	t.Cleanup(func() {
		_ = src.Teardown(ctx)
	})

	require.ErrorContains(t, src.Open(ctx, nil), "connector open error: could not create container connection client")
}

func TestSource_FailsWhenRecordPositionIsInvalid(t *testing.T) {
	ctx := context.Background()

	var (
		containerName = "source-integration-tests"

		cfgRaw = map[string]string{
			ConfigKeyConnectionString: helper.GetConnectionString(),
			ConfigKeyContainerName:    containerName,
			ConfigKeyPollingPeriod:    "1500ms",
			ConfigKeyMaxResults:       "1",
		}
	)

	helper.PrepareContainer(t, helper.NewAzureBlobClient(), containerName)

	src := NewSource()

	require.NoError(t, src.Configure(ctx, cfgRaw))

	t.Cleanup(func() {
		_ = src.Teardown(ctx)
	})

	require.EqualError(t, src.Open(ctx, opencdc.Position("invalid gob object")), "connector open error: invalid or unsupported position: unexpected EOF")
}

func TestSource_ReadsContainerItemsWithSnapshotIteratorAndThenReadsAddedItemsWithCDCIterator(t *testing.T) {
	ctx := context.Background()
	fakerInstance := faker.New()

	var (
		containerName = "source-integration-tests"

		cfgRaw = map[string]string{
			ConfigKeyConnectionString: helper.GetConnectionString(),
			ConfigKeyContainerName:    containerName,
			ConfigKeyPollingPeriod:    "1500ms",
			ConfigKeyMaxResults:       "1",
		}
	)

	azureBlobClient := helper.NewAzureBlobClient()
	helper.PrepareContainer(t, azureBlobClient, containerName)

	var (
		alreadyExistingBlob1Name        = "already-existing-1.txt"
		alreadyExistingBlob1ContentType = "text/plain; charset=utf-8"
		alreadyExistingBlob1Contents    = fakerInstance.Lorem().Sentence(16)

		alreadyExistingBlob2Name        = "already-existing-2.txt"
		alreadyExistingBlob2ContentType = "text/plain; charset=utf-8"
		alreadyExistingBlob2Contents    = fakerInstance.Lorem().Sentence(16)

		createdWhileWorking1Name        = "created-while-running-1.txt"
		createdWhileWorking1ContentType = "text/plain; charset=utf-8"
		createdWhileWorking1Contents    = fakerInstance.Lorem().Sentence(16)
	)

	require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, alreadyExistingBlob1Name, alreadyExistingBlob1ContentType, alreadyExistingBlob1Contents))
	require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, alreadyExistingBlob2Name, alreadyExistingBlob2ContentType, alreadyExistingBlob2Contents))

	time.Sleep(time.Second)

	src := NewSource()

	require.NoError(t, src.Configure(ctx, cfgRaw))
	require.NoError(t, src.Open(ctx, nil))

	t.Cleanup(func() {
		_ = src.Teardown(ctx)
	})

	t.Run("Firstly, snapshot iterator runs and reads all blobs", func(t *testing.T) {
		// Read the first record from the first page
		record1, err := src.Read(ctx)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record1, alreadyExistingBlob1Name, alreadyExistingBlob1ContentType, alreadyExistingBlob1Contents))
		require.NoError(t, src.Ack(ctx, record1.Position))

		// Read the second record from the second page
		record2, err := src.Read(ctx)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record2, alreadyExistingBlob2Name, alreadyExistingBlob2ContentType, alreadyExistingBlob2Contents))
		require.NoError(t, src.Ack(ctx, record2.Position))

		// No third record available, backoff
		// By this time, combined iterator should switch from snapshot to CDC iterator
		record3, err := src.Read(ctx)
		require.Equal(t, opencdc.Record{}, record3)
		require.ErrorIs(t, err, sdk.ErrBackoffRetry)
	})

	t.Run("Secondly, CDC iterator runs and reads all blobs created meanwhile", func(t *testing.T) {
		// Create file while snapshot iterator is working
		// This file should not be included in the results
		require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, createdWhileWorking1Name, createdWhileWorking1ContentType, createdWhileWorking1Contents))

		// No new record is available until 1 polling period passes
		record1, err := src.Read(ctx)
		require.Equal(t, opencdc.Record{}, record1)
		require.ErrorIs(t, err, sdk.ErrBackoffRetry)

		// Polling Period is 1.5s
		time.Sleep(time.Second * 2)

		// Read the first record polled
		record2, err := src.Read(ctx)
		require.NoError(t, err)
		require.True(t, helper.AssertRecordEquals(t, record2, createdWhileWorking1Name, createdWhileWorking1ContentType, createdWhileWorking1Contents))
		require.NoError(t, src.Ack(ctx, record2.Position))

		// Read the second record polled
		record3, err := src.Read(ctx)
		require.Equal(t, opencdc.Record{}, record3)
		require.ErrorIs(t, err, sdk.ErrBackoffRetry)
	})
}

func TestSource_SnapshotIteratorReadsEmptyContainerAndThenSwitchedToCDCIterator(t *testing.T) {
	ctx := context.Background()

	var (
		containerName = "source-integration-tests"

		cfgRaw = map[string]string{
			ConfigKeyConnectionString: helper.GetConnectionString(),
			ConfigKeyContainerName:    containerName,
			ConfigKeyPollingPeriod:    "1500ms",
			ConfigKeyMaxResults:       "1",
		}
	)

	helper.PrepareContainer(t, helper.NewAzureBlobClient(), containerName)

	src := NewSource()

	require.NoError(t, src.Configure(ctx, cfgRaw))
	require.NoError(t, src.Open(ctx, nil))

	t.Cleanup(func() {
		_ = src.Teardown(ctx)
	})

	t.Run("Firstly, snapshot iterator runs and reads properly empty container", func(t *testing.T) {
		record, err := src.Read(ctx)
		require.Equal(t, opencdc.Record{}, record)
		require.ErrorIs(t, err, sdk.ErrBackoffRetry)
	})

	t.Run("Secondly, snapshot iterator switches to CDC iterator", func(t *testing.T) {
		record, err := src.Read(ctx)
		require.Equal(t, opencdc.Record{}, record)
		require.ErrorIs(t, err, sdk.ErrBackoffRetry)
	})

	t.Run("Thirdly, CDC iterator runs and reads properly empty container", func(t *testing.T) {
		record, err := src.Read(ctx)
		require.Equal(t, opencdc.Record{}, record)
		require.ErrorIs(t, err, sdk.ErrBackoffRetry)
	})
}

func TestSource_CDCIteratorOmitsAlreadyReadItems(t *testing.T) {
	ctx := context.Background()
	fakerInstance := faker.New()

	var (
		containerName = "source-integration-tests"

		cfgRaw = map[string]string{
			ConfigKeyConnectionString: helper.GetConnectionString(),
			ConfigKeyContainerName:    containerName,
			ConfigKeyPollingPeriod:    "100ms",
			ConfigKeyMaxResults:       "100",
		}
	)

	azureBlobClient := helper.NewAzureBlobClient()
	helper.PrepareContainer(t, azureBlobClient, containerName)

	var (
		alreadyExistingBlob1Name        = "already-existing-1.txt"
		alreadyExistingBlob1ContentType = "text/plain; charset=utf-8"
		alreadyExistingBlob1Contents    = fakerInstance.Lorem().Sentence(16)

		alreadyExistingBlob2Name        = "already-existing-2.txt"
		alreadyExistingBlob2ContentType = "text/plain; charset=utf-8"
		alreadyExistingBlob2Contents    = fakerInstance.Lorem().Sentence(16)

		createdWhileWorking1Name        = "created-while-running-1.txt"
		createdWhileWorking1ContentType = "text/plain; charset=utf-8"
		createdWhileWorking1Contents    = fakerInstance.Lorem().Sentence(16)
	)

	require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, alreadyExistingBlob1Name, alreadyExistingBlob1ContentType, alreadyExistingBlob1Contents))
	require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, alreadyExistingBlob2Name, alreadyExistingBlob2ContentType, alreadyExistingBlob2Contents))

	time.Sleep(time.Second)

	recordPosition, err := position.NewCDCPosition("", time.Now()).ToRecordPosition()
	require.NoError(t, err)

	time.Sleep(time.Second)

	require.NoError(t, helper.CreateBlob(azureBlobClient, containerName, createdWhileWorking1Name, createdWhileWorking1ContentType, createdWhileWorking1Contents))

	src := NewSource()

	require.NoError(t, src.Configure(ctx, cfgRaw))
	require.NoError(t, src.Open(ctx, recordPosition))

	t.Cleanup(func() {
		_ = src.Teardown(ctx)
	})

	time.Sleep(time.Second)

	record1, err := src.Read(ctx)
	require.True(t, helper.AssertRecordEquals(t, record1, createdWhileWorking1Name, createdWhileWorking1ContentType, createdWhileWorking1Contents))
	require.NoError(t, src.Ack(ctx, record1.Position))

	record2, err := src.Read(ctx)
	require.Equal(t, opencdc.Record{}, record2)
	require.ErrorIs(t, err, sdk.ErrBackoffRetry)
}
