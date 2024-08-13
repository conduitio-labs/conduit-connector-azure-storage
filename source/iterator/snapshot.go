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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/miquido/conduit-connector-azure-storage/source/position"
	"gopkg.in/tomb.v2"
)

var ErrSnapshotIteratorIsStopped = errors.New("snapshot iterator is stopped")

func NewSnapshotIterator(
	client *container.Client,
	p position.Position,
	maxResults int32,
) (*SnapshotIterator, error) {
	if maxResults < 1 {
		return nil, fmt.Errorf("maxResults is expected to be greater than or equal to 1, got %d", maxResults)
	}

	iterator := SnapshotIterator{
		client: client,
		paginator: client.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
			MaxResults: &maxResults,
		}),
		maxLastModified: p.Timestamp,
		buffer:          make(chan opencdc.Record, 1),
		tomb:            tomb.Tomb{},
	}

	iterator.tomb.Go(iterator.producer)

	return &iterator, nil
}

type SnapshotIterator struct {
	client          *container.Client
	paginator       *runtime.Pager[azblob.ListBlobsFlatResponse]
	maxLastModified time.Time
	buffer          chan opencdc.Record
	tomb            tomb.Tomb
}

func (w *SnapshotIterator) HasNext(_ context.Context) bool {
	return w.tomb.Alive() || len(w.buffer) > 0
}

func (w *SnapshotIterator) Next(ctx context.Context) (opencdc.Record, error) {
	select {
	case r, active := <-w.buffer:
		if !active {
			return opencdc.Record{}, ErrSnapshotIteratorIsStopped
		}

		return r, nil

	case <-w.tomb.Dead():
		err := w.tomb.Err()
		if err == nil {
			err = ErrSnapshotIteratorIsStopped
		}

		return opencdc.Record{}, err

	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	}
}

func (w *SnapshotIterator) Stop() {
	w.tomb.Kill(ErrSnapshotIteratorIsStopped)
	_ = w.tomb.Wait()
}

// producer reads the container and reports all files found.
func (w *SnapshotIterator) producer() error {
	defer close(w.buffer)

	ctx := w.tomb.Context(nil) //nolint:staticcheck // tomb.Context expects nil as argument

	for w.paginator.More() {
		resp, err := w.paginator.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, item := range resp.Segment.BlobItems {
			// Check if maxLastModified should be updated
			if w.maxLastModified.Before(*item.Properties.LastModified) {
				w.maxLastModified = *item.Properties.LastModified
			}

			// Read the contents of the item
			blobClient := w.client.NewBlobClient(*item.Name)
			downloadResponse, err := blobClient.DownloadStream(ctx, nil)
			if err != nil {
				return err
			}

			record, err := w.createSnapshotRecord(ctx, item, downloadResponse)
			if err != nil {
				return err
			}

			// Send out the record if possible
			select {
			case w.buffer <- record:
				// opencdc.Record was sent successfully

			case <-w.tomb.Dying():
				return nil
			}
		}
	}
	return nil
}

// createSnapshotRecord converts blob item into opencdc.Record with item's contents.
func (w *SnapshotIterator) createSnapshotRecord(ctx context.Context, item *container.BlobItem, resp azblob.DownloadStreamResponse) (opencdc.Record, error) {
	// Try to read item's contents
	reader := resp.NewRetryReader(ctx, &blob.RetryReaderOptions{
		MaxRetries: 3,
	})
	defer reader.Close()

	rawBody, err := io.ReadAll(reader)
	if err != nil {
		return opencdc.Record{}, err
	}

	// Prepare the record position
	p := position.NewSnapshotPosition(*item.Name, w.maxLastModified)

	recordPosition, err := p.ToRecordPosition()
	if err != nil {
		return opencdc.Record{}, err
	}

	// Prepare the opencdc.Record
	metadata := make(opencdc.Metadata)
	metadata.SetCreatedAt(*item.Properties.CreationTime)
	metadata["azure-storage.content-type"] = *item.Properties.ContentType

	return sdk.Util.Source.NewRecordSnapshot(
		recordPosition, metadata, opencdc.RawData(*item.Name), opencdc.RawData(rawBody),
	), nil
}
