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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/miquido/conduit-connector-azure-storage/source/position"
)

type CombinedIterator struct {
	snapshotIterator *SnapshotIterator
	cdcIterator      *CDCIterator

	pollingPeriod time.Duration
	client        *azblob.ContainerClient
	maxResults    int32
}

func NewCombinedIterator(
	pollingPeriod time.Duration,
	client *azblob.ContainerClient,
	maxResults int32,
	p position.Position,
) (c *CombinedIterator, err error) {
	c = &CombinedIterator{
		pollingPeriod: pollingPeriod,
		client:        client,
		maxResults:    maxResults,
	}

	switch p.Type {
	case position.TypeSnapshot:
		if len(p.Key) != 0 {
			fmt.Printf("Warning: got position: %+v, snapshot will be restarted from the beginning of the bucket\n", p)
		}

		p = position.NewSnapshotPosition() // always start snapshot from the beginning, so position is nil

		c.snapshotIterator, err = NewSnapshotIterator(client, p, maxResults)
		if err != nil {
			return nil, fmt.Errorf("could not create the snapshot iterator: %w", err)
		}

	case position.TypeCDC:
		c.cdcIterator, err = NewCDCIterator(pollingPeriod, client, p.Timestamp, maxResults)
		if err != nil {
			return nil, fmt.Errorf("could not create the CDC iterator: %w", err)
		}

	default:
		return nil, fmt.Errorf("invalid position type (%d)", p.Type)
	}

	return c, nil
}

func (c *CombinedIterator) HasNext(ctx context.Context) bool {
	switch {
	case c.snapshotIterator != nil:
		// case of empty bucket or end of bucket
		if !c.snapshotIterator.HasNext(ctx) {
			err := c.switchToCDCIterator()
			if err != nil {
				return false
			}

			return false
		}

		return true

	case c.cdcIterator != nil:
		return c.cdcIterator.HasNext(ctx)

	default:
		return false
	}
}

func (c *CombinedIterator) Next(ctx context.Context) (sdk.Record, error) {
	switch {
	case c.snapshotIterator != nil:
		r, err := c.snapshotIterator.Next(ctx)
		if err != nil {
			return sdk.Record{}, err
		}

		if !c.snapshotIterator.HasNext(ctx) {
			// Switch to CDC iterator
			err := c.switchToCDCIterator()
			if err != nil {
				return sdk.Record{}, err
			}

			// Change the last record's position to CDC
			r.Position, err = convertToCDCPosition(r.Position)
			if err != nil {
				return sdk.Record{}, err
			}
		}

		return r, nil

	case c.cdcIterator != nil:
		return c.cdcIterator.Next(ctx)

	default:
		return sdk.Record{}, errors.New("no initialized iterator")
	}
}

func (c *CombinedIterator) Stop() {
	if c.cdcIterator != nil {
		c.cdcIterator.Stop()
	}

	if c.snapshotIterator != nil {
		c.snapshotIterator.Stop()
	}
}

func (c *CombinedIterator) switchToCDCIterator() (err error) {
	timestamp := c.snapshotIterator.maxLastModified

	// Zero timestamp means nil position (empty bucket), so start detecting actions from now
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	c.cdcIterator, err = NewCDCIterator(c.pollingPeriod, c.client, timestamp.Add(time.Nanosecond), c.maxResults)
	if err != nil {
		return fmt.Errorf("could not create cdc iterator: %w", err)
	}

	c.snapshotIterator = nil

	return nil
}

func convertToCDCPosition(p sdk.Position) (sdk.Position, error) {
	cdcPos, err := position.NewFromRecordPosition(p)
	if err != nil {
		return sdk.Position{}, err
	}

	cdcPos.Type = position.TypeCDC

	return cdcPos.ToRecordPosition()
}
