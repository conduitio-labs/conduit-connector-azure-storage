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
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/miquido/conduit-connector-azure-storage/internal"
	"github.com/miquido/conduit-connector-azure-storage/source/position"
	"gopkg.in/tomb.v2"
)

var ErrSnapshotIteratorIsStopped = errors.New("snapshot iterator is stopped")

func NewSnapshotIterator(
	client *azblob.ContainerClient,
	p position.Position,
	maxResults int32,
) (*SnapshotIterator, error) {
	if maxResults < 1 {
		return nil, fmt.Errorf("maxResults is expected to be greater than or equal to 1, got %d", maxResults)
	}

	iterator := SnapshotIterator{
		client: client,
		paginator: client.ListBlobsFlat(&azblob.ContainerListBlobsFlatOptions{
			MaxResults: &maxResults,
		}),
		maxLastModified: p.Timestamp,
		buffer:          make(chan sdk.Record, 1),
		tomb:            tomb.Tomb{},
	}

	iterator.tomb.Go(iterator.producer)

	return &iterator, nil
}

type SnapshotIterator struct {
	client          *azblob.ContainerClient
	paginator       *azblob.ContainerListBlobFlatPager
	maxLastModified time.Time
	buffer          chan sdk.Record
	tomb            tomb.Tomb
}

func (w *SnapshotIterator) HasNext(_ context.Context) bool {
	return w.tomb.Alive() || len(w.buffer) > 0
}

func (w *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case r, active := <-w.buffer:
		if !active {
			return sdk.Record{}, ErrSnapshotIteratorIsStopped
		}

		return r, nil

	case <-w.tomb.Dead():
		err := w.tomb.Err()
		if err == nil {
			err = ErrSnapshotIteratorIsStopped
		}

		return sdk.Record{}, err

	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

func (w *SnapshotIterator) Stop() {
	w.tomb.Kill(ErrSnapshotIteratorIsStopped)
	_ = w.tomb.Wait()
}

// producer reads the container and reports all files found.
func (w *SnapshotIterator) producer() error {
	defer close(w.buffer)

	ctx := context.Background()

	for {
		if w.paginator.NextPage(w.tomb.Context(ctx)) {
			resp := w.paginator.PageResponse()

			for _, item := range resp.Segment.BlobItems {
				// Check if maxLastModified should be updated
				if w.maxLastModified.Before(*item.Properties.LastModified) {
					w.maxLastModified = *item.Properties.LastModified
				}

				// Read the contents of the item
				blobClient, err := w.client.NewBlobClient(*item.Name)
				if err != nil {
					return err
				}

				downloadResponse, err := blobClient.Download(w.tomb.Context(ctx), nil)
				if err != nil {
					return err
				}

				rawBody, err := io.ReadAll(downloadResponse.Body(&azblob.RetryReaderOptions{
					MaxRetryRequests: 0,
				}))
				if err != nil {
					return err
				}

				// Prepare the record position
				p := position.NewSnapshotPosition(*item.Name, w.maxLastModified)

				recordPosition, err := p.ToRecordPosition()
				if err != nil {
					return err
				}

				// Prepare the sdk.Record
				record := sdk.Record{
					Metadata: map[string]string{
						"action":       internal.OperationInsert,
						"content-type": *item.Properties.ContentType,
					},
					Position:  recordPosition,
					Payload:   sdk.RawData(rawBody),
					Key:       sdk.RawData(*item.Name),
					CreatedAt: *item.Properties.CreationTime,
				}

				// Send out the record if possible
				select {
				case w.buffer <- record:
					// sdk.Record was sent successfully

				case <-w.tomb.Dying():
					return nil
				}
			}

			continue
		}

		// Report a storage reading error
		if err := w.paginator.Err(); err != nil {
			return err
		}

		return nil
	}
}
