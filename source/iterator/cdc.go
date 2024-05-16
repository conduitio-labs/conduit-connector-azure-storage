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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/miquido/conduit-connector-azure-storage/source/position"
	"gopkg.in/tomb.v2"
)

var ErrCDCIteratorIsStopped = errors.New("CDC iterator is stopped")

func NewCDCIterator(
	pollingPeriod time.Duration,
	client *container.Client,
	from time.Time,
	maxResults int32,
) (*CDCIterator, error) {
	if maxResults < 1 {
		return nil, fmt.Errorf("maxResults is expected to be greater than or equal to 1, got %d", maxResults)
	}

	cdc := CDCIterator{
		client:        client,
		buffer:        make(chan sdk.Record, 1),
		ticker:        time.NewTicker(pollingPeriod),
		isTruncated:   true,
		nextKeyMarker: nil,
		tomb:          tomb.Tomb{},
		lastModified:  from,
		maxResults:    maxResults,
	}

	cdc.tomb.Go(cdc.producer)

	return &cdc, nil
}

type CDCIterator struct {
	client        *container.Client
	buffer        chan sdk.Record
	ticker        *time.Ticker
	lastModified  time.Time
	maxResults    int32
	isTruncated   bool
	nextKeyMarker *string
	tomb          tomb.Tomb
}

func (w *CDCIterator) HasNext(_ context.Context) bool {
	return len(w.buffer) > 0 || !w.tomb.Alive() // if tomb is dead we return true so caller will fetch error with Next
}

func (w *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case r, active := <-w.buffer:
		if !active {
			return sdk.Record{}, ErrCDCIteratorIsStopped
		}

		return r, nil

	case <-w.tomb.Dead():
		return sdk.Record{}, w.tomb.Err()

	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

func (w *CDCIterator) Stop() {
	w.ticker.Stop()
	w.tomb.Kill(ErrCDCIteratorIsStopped)
	_ = w.tomb.Wait()
}

// producer reads the container and reports all file changes since last time.
func (w *CDCIterator) producer() error {
	defer close(w.buffer)

	for {
		select {
		case <-w.tomb.Dying():
			return w.tomb.Err()

		case <-w.ticker.C:
			currentLastModified := w.lastModified

			// Prepare the storage iterator
			blobListPager := w.client.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
				Marker:     w.nextKeyMarker,
				MaxResults: &w.maxResults,
				Include: azblob.ListBlobsInclude{
					Deleted: true,
				},
			})

			ctx := w.tomb.Context(nil) //nolint:staticcheck // tomb.Context expects nil as argument

			for blobListPager.More() {
				resp, err := blobListPager.NextPage(ctx)
				if err != nil {
					return err
				}

				for _, item := range resp.Segment.BlobItems {
					itemLastModificationDate := *item.Properties.LastModified
					itemDeletionTime := item.Properties.DeletedTime

					// Reject item when it wasn't modified or deleted since the last iteration
					if itemLastModificationDate.Before(w.lastModified) {
						if itemDeletionTime != nil && itemDeletionTime.After(w.lastModified) {
							itemLastModificationDate = *itemDeletionTime
						} else {
							continue
						}
					}

					// Prepare the sdk.Record
					var output sdk.Record

					if item.Deleted != nil && *item.Deleted {
						var err error

						output, err = w.createDeletedRecord(item)
						if err != nil {
							return err
						}
					} else {
						blobClient := w.client.NewBlobClient(*item.Name)
						downloadResponse, err := blobClient.DownloadStream(ctx, nil)
						if err != nil {
							return err
						}

						output, err = w.createUpsertedRecord(ctx, item, downloadResponse)
						if err != nil {
							return err
						}
					}

					// Send out the record if possible
					select {
					case <-w.tomb.Dying():
						return w.tomb.Err()

					case w.buffer <- output:
						if currentLastModified.Before(itemLastModificationDate) {
							currentLastModified = itemLastModificationDate
						}
					}
				}
			}

			// Update times
			w.lastModified = currentLastModified.Add(time.Nanosecond)
		}
	}
}

// createUpsertedRecord converts blob item into sdk.Record with item's contents or returns error when failure.
func (w *CDCIterator) createUpsertedRecord(ctx context.Context, item *container.BlobItem, resp azblob.DownloadStreamResponse) (sdk.Record, error) {
	// Try to read item's contents
	reader := resp.NewRetryReader(ctx, &blob.RetryReaderOptions{
		MaxRetries: 3,
	})
	defer reader.Close()

	rawBody, err := io.ReadAll(reader)
	if err != nil {
		return sdk.Record{}, err
	}

	// Prepare position information
	p := position.NewCDCPosition(*item.Name, *item.Properties.LastModified)

	recordPosition, err := p.ToRecordPosition()
	if err != nil {
		return sdk.Record{}, err
	}

	// Prepare metadata
	metadata := make(sdk.Metadata)
	metadata.SetCreatedAt(p.Timestamp)
	metadata["azure-storage.content-type"] = *resp.ContentType

	if item.Properties.CreationTime == nil || item.Properties.LastModified == nil || item.Properties.CreationTime.Equal(*item.Properties.LastModified) {
		return sdk.Util.Source.NewRecordCreate(
			recordPosition, metadata, sdk.RawData(p.Key), sdk.RawData(rawBody),
		), nil
	}

	return sdk.Util.Source.NewRecordUpdate(
		recordPosition, metadata, sdk.RawData(p.Key), nil, sdk.RawData(rawBody),
	), nil
}

// createDeletedRecord converts blob item into sdk.Record indicating that item was removed or returns error
// when failure.
func (w *CDCIterator) createDeletedRecord(item *container.BlobItem) (sdk.Record, error) {
	// Prepare position information
	p := position.NewCDCPosition(*item.Name, *item.Properties.LastModified)

	recordPosition, err := p.ToRecordPosition()
	if err != nil {
		return sdk.Record{}, err
	}

	// Prepare metadata
	metadata := make(sdk.Metadata)
	metadata.SetCreatedAt(p.Timestamp)

	// Return the record
	return sdk.Util.Source.NewRecordDelete(recordPosition, metadata, sdk.RawData(p.Key)), nil
}
