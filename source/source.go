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
	"net/http"
	"reflect"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
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
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		ConfigKeyConnectionString: {
			Default:     "",
			Required:    true,
			Description: "The Azure Storage connection string.",
		},
		ConfigKeyContainerName: {
			Default:     "",
			Required:    true,
			Description: "The name of the container to monitor.",
		},
		ConfigKeyPollingPeriod: {
			Default:     DefaultPollingPeriod,
			Required:    false,
			Description: "The polling period for the CDC mode, formatted as a time.Duration string.",
		},
		ConfigKeyMaxResults: {
			Default:     strconv.FormatInt(int64(DefaultMaxResults), 32),
			Required:    false,
			Description: "The maximum number of items, per page, when reading container's items.",
		},
	}
}

func (s *Source) Configure(_ context.Context, cfgRaw map[string]string) (err error) {
	s.config, err = ParseConfig(cfgRaw)
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	return nil
}

func (s *Source) Open(ctx context.Context, rp sdk.Position) error {
	// Create account connection client
	serviceClient, err := azblob.NewServiceClientFromConnectionString(s.config.ConnectionString, nil)
	if err != nil {
		return fmt.Errorf("connector open error: could not create account connection client: %w", err)
	}

	// Test account connection
	accountInfo, err := serviceClient.GetAccountInfo(ctx, nil)
	if err != nil {
		return fmt.Errorf("connector open error: could not establish a connection: %w", err)
	}
	if accountInfo.RawResponse.StatusCode != http.StatusOK {
		return fmt.Errorf("connector open error: could not establish a connection: unexpected response status %d", accountInfo.RawResponse.StatusCode)
	}

	// Create container client
	containerClient, err := serviceClient.NewContainerClient(s.config.ContainerName)
	if err != nil {
		return fmt.Errorf("connector open error: could not create container connection client: %w", err)
	}

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

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	if !s.iterator.HasNext(ctx) {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		if errors.Is(err, iterator.ErrSnapshotIteratorIsStopped) {
			return sdk.Record{}, sdk.ErrBackoffRetry
		}

		return sdk.Record{}, fmt.Errorf("read error: %w", err)
	}

	// Case when new record could not be produced but no error was thrown at the same time
	// E.g.: goroutine stopped and w.tomb.Err() returned empty record and nil error
	if reflect.DeepEqual(record, sdk.Record{}) {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	return record, nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
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
