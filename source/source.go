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

package source

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/miquido/conduit-connector-azure-storage/source/iterator"
	"github.com/miquido/conduit-connector-azure-storage/source/position"
)

type Source struct {
	sdk.UnimplementedSource

	config   Config
	iterator iterator.Iterator
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters is a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() config.Parameters {
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg config.Config) error {
	return sdk.Util.ParseConfig(ctx, cfg, &s.config, NewSource().Parameters())
}

func (s *Source) Open(ctx context.Context, rp opencdc.Position) error {
	serviceClient, err := service.NewClientFromConnectionString(s.config.ConnectionString, nil)
	if err != nil {
		return fmt.Errorf("connector open error: could not create account connection client: %w", err)
	}

	// Test account connection
	_, err = serviceClient.GetAccountInfo(ctx, nil)
	if err != nil {
		return fmt.Errorf("connector open error: could not establish a connection: %w", err)
	}

	// Create container client
	containerClient := serviceClient.NewContainerClient(s.config.ContainerName)

	// Check if container exists
	_, err = containerClient.GetProperties(ctx, nil)
	if err != nil {
		return fmt.Errorf("connector open error: could not create container connection client: %w", err)
	}

	// Parse position to start from
	recordPosition, err := position.NewFromRecordPosition(rp)
	if err != nil {
		return fmt.Errorf("connector open error: invalid or unsupported position: %w", err)
	}

	// Create container's items iterator
	s.iterator, err = iterator.NewCombinedIterator(s.config.PollingPeriod, containerClient, s.config.MaxResults, recordPosition)
	if err != nil {
		return fmt.Errorf("connector open error: couldn't create a combined iterator: %w", err)
	}

	return nil
}

func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	if !s.iterator.HasNext(ctx) {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		if errors.Is(err, iterator.ErrSnapshotIteratorIsStopped) {
			return opencdc.Record{}, sdk.ErrBackoffRetry
		}

		return opencdc.Record{}, fmt.Errorf("read error: %w", err)
	}

	// Case when new record could not be produced but no error was thrown at the same time
	// E.g.: goroutine stopped and w.tomb.Err() returned empty record and nil error
	if reflect.DeepEqual(record, opencdc.Record{}) {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	return record, nil
}

func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")

	return nil // no ack needed
}

func (s *Source) Teardown(_ context.Context) error {
	if s.iterator != nil {
		s.iterator.Stop()
		s.iterator = nil
	}

	return nil
}
