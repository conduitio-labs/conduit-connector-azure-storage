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

package test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub#well-known-storage-account-and-key
const (
	defaultEndpointsProtocol = "http"
	accountName              = "devstoreaccount1"
	accountKey               = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

var (
	azureBlobClient          *azblob.Client
	createNewAzureBlobClient sync.Once

	// https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub#http-connection-strings
	connectionString = fmt.Sprintf(
		"DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s",
		defaultEndpointsProtocol,
		accountName,
		accountKey,
		fmt.Sprintf("http://127.0.0.1:10000/%s", accountName),
	)
)

func GetConnectionString() string {
	return connectionString
}

func NewAzureBlobClient() *azblob.Client {
	createNewAzureBlobClient.Do(func() {
		var err error
		azureBlobClient, err = azblob.NewClientFromConnectionString(connectionString, nil)

		if err != nil {
			panic(fmt.Errorf("failed to create Azure Blob Service Client: %w", err))
		}
	})

	return azureBlobClient
}

func PrepareContainer(t *testing.T, client *azblob.Client, containerName string) *container.Client {
	t.Cleanup(func() {
		_, _ = client.DeleteContainer(context.Background(), containerName, nil)
	})

	_, err := client.CreateContainer(context.Background(), containerName, nil)
	require.NoError(t, err)

	return client.ServiceClient().NewContainerClient(containerName)
}

func CreateBlob(client *azblob.Client, containerName, blobName, contentType, contents string) error {
	_, err := client.UploadBuffer(
		context.Background(),
		containerName,
		blobName,
		[]byte(contents),
		&azblob.UploadBufferOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentType: to.Ptr(contentType),
			},
		},
	)
	return err
}

func AssertRecordEquals(t *testing.T, record sdk.Record, fileName, contentType, contents string) bool {
	return assert.NotNil(t, record.Key, "Record Key is not set.") &&
		assert.NotNil(t, record.Payload, "Record Payload is not set.") &&
		assert.Equal(t, fileName, string(record.Key.Bytes()), "Record name does not match.") &&
		assert.Equal(t, contentType, record.Metadata["azure-storage.content-type"], "Record's content-type metadata does not match.") &&
		assert.Equal(t, contents, string(record.Payload.After.Bytes()), "Record payload does not match.")
}
