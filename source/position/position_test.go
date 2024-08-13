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

//go:build unit

package position

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jaswdr/faker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFromRecordPosition(t *testing.T) {
	fakerInstance := faker.New()

	t.Run("Fails when opencdc.Position cannot be decoded into Position", func(t *testing.T) {
		_, err := NewFromRecordPosition(opencdc.Position("invalid binary data"))

		require.EqualError(t, err, "unexpected EOF")
	})

	t.Run("Returns default Snapshot Position when opencdc.Position is nil", func(t *testing.T) {
		position, err := NewFromRecordPosition(nil)

		require.NoError(t, err)
		require.True(t, assertPositionsAreEqual(t, NewDefaultSnapshotPosition(), position))
	})

	t.Run("Returns Position when opencdc.Position was successfully decoded", func(t *testing.T) {
		p := Position{
			Key: fakerInstance.Lorem().Sentence(6),
			Timestamp: fakerInstance.Time().TimeBetween(
				time.Now().AddDate(-1, 0, 0),
				time.Now().AddDate(1, 0, 0),
			),
			Type: TypeCDC,
		}

		recordPosition, err := p.ToRecordPosition()
		require.NoError(t, err)

		position, err := NewFromRecordPosition(recordPosition)

		require.NoError(t, err)
		require.True(t, assertPositionsAreEqual(t, p, position))
	})
}

func TestNewDefaultSnapshotPosition(t *testing.T) {
	t.Run("New default Snapshot Position is created", func(t *testing.T) {
		require.True(t, assertPositionsAreEqual(t, Position{Type: TypeSnapshot}, NewDefaultSnapshotPosition()))
	})
}

func TestNewSnapshotPosition(t *testing.T) {
	fakerInstance := faker.New()

	t.Run("Position with given key and timestamp is created", func(t *testing.T) {
		var (
			PositionKey       = fakerInstance.Lorem().Sentence(6)
			PositionTimestamp = fakerInstance.Time().TimeBetween(
				time.Now().AddDate(-1, 0, 0),
				time.Now().AddDate(1, 0, 0),
			)
		)

		require.True(t, assertPositionsAreEqual(t, Position{
			Key:       PositionKey,
			Timestamp: PositionTimestamp,
			Type:      TypeSnapshot,
		}, NewSnapshotPosition(PositionKey, PositionTimestamp)))
	})
}

func TestNewCDCPosition(t *testing.T) {
	fakerInstance := faker.New()

	t.Run("Position with given key and timestamp is created", func(t *testing.T) {
		var (
			PositionKey       = fakerInstance.Lorem().Sentence(6)
			PositionTimestamp = fakerInstance.Time().TimeBetween(
				time.Now().AddDate(-1, 0, 0),
				time.Now().AddDate(1, 0, 0),
			)
		)

		require.True(t, assertPositionsAreEqual(t, Position{
			Key:       PositionKey,
			Timestamp: PositionTimestamp,
			Type:      TypeCDC,
		}, NewCDCPosition(PositionKey, PositionTimestamp)))
	})
}

func TestPosition_ToRecordPosition(t *testing.T) {
	fakerInstance := faker.New()

	t.Run("Encoded Position is decoded successfully", func(t *testing.T) {
		var (
			PositionKey       = fakerInstance.Lorem().Sentence(6)
			PositionTimestamp = fakerInstance.Time().TimeBetween(
				time.Now().AddDate(-1, 0, 0),
				time.Now().AddDate(1, 0, 0),
			)
		)

		position := Position{
			Key:       PositionKey,
			Timestamp: PositionTimestamp,
			Type:      TypeCDC,
		}

		recordPosition, err := position.ToRecordPosition()
		require.NoError(t, err)

		var buff bytes.Buffer
		buff.Write(recordPosition)

		var positionDecoded Position
		require.NoError(t, gob.NewDecoder(&buff).Decode(&positionDecoded))
		require.True(t, assertPositionsAreEqual(t, position, positionDecoded))
	})
}

func assertPositionsAreEqual(t *testing.T, expected, actual Position) bool {
	return assert.Equal(t, expected.Type, actual.Type) &&
		assert.Equal(t, expected.Key, actual.Key) &&
		assert.Equal(t, expected.Timestamp.Truncate(time.Microsecond), actual.Timestamp.Truncate(time.Microsecond))
}
