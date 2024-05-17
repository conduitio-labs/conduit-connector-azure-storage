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

package azure

import (
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/miquido/conduit-connector-azure-storage/source"
	helper "github.com/miquido/conduit-connector-azure-storage/test"
	"go.uber.org/goleak"
)

type CustomConfigurableAcceptanceTestDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
	blobClient    *azblob.Client
	containerName string
}

func (d *CustomConfigurableAcceptanceTestDriver) GenerateRecord(t *testing.T, op sdk.Operation) sdk.Record {
	record := d.ConfigurableAcceptanceTestDriver.GenerateRecord(t, op)

	// Override Key
	record.Key = sdk.RawData(fmt.Sprintf("file-%d.txt", time.Now().UnixMicro()))

	return record
}

func (d *CustomConfigurableAcceptanceTestDriver) WriteToSource(_ *testing.T, records []sdk.Record) (results []sdk.Record) {
	for _, record := range records {
		_ = helper.CreateBlob(
			d.blobClient,
			d.containerName,
			string(record.Key.Bytes()),
			"text/plain",
			string(record.Payload.After.Bytes()),
		)
	}

	// No destination connector, return wanted records
	return records
}

func TestAcceptance(t *testing.T) {
	sourceConfig := map[string]string{
		source.ConfigKeyConnectionString: helper.GetConnectionString(),
		source.ConfigKeyContainerName:    "acceptance-tests",
		source.ConfigKeyMaxResults:       "5000",
		source.ConfigKeyPollingPeriod:    "1s",
	}

	testDriver := CustomConfigurableAcceptanceTestDriver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector: Connector,

				SourceConfig:     sourceConfig,
				GenerateDataType: sdk.GenerateRawData,

				GoleakOptions: []goleak.Option{
					// Routines created by Azure client
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
					goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
				},

				Skip: []string{
					// Cannot be tested. driver.WriteToSource() creates all records at once while CDC position is
					// created after the Snapshot iterator finishes iterating. So from the CDC iterator point of view,
					// there are no new records available.
					"TestAcceptance/TestSource_Open_ResumeAtPositionCDC",

					// Same as above: due to the nature of the CDC iterator, which runs after snapshot iterator,
					// there are no changes left to detect.
					// Only `TestAcceptance/TestSource_Read_Success/cdc` fails, but it cannot be excluded alone.
					"TestAcceptance/TestSource_Read_Success",
					"TestAcceptance/TestSource_Configure_RequiredParams",
				},
			},
		},
	}

	testDriver.ConfigurableAcceptanceTestDriver.Config.BeforeTest = func(t *testing.T) {
		blobClient := helper.NewAzureBlobClient()
		helper.PrepareContainer(t, blobClient, sourceConfig[source.ConfigKeyContainerName])
		testDriver.blobClient = blobClient
		testDriver.containerName = sourceConfig[source.ConfigKeyContainerName]
	}

	sdk.AcceptanceTest(t, &testDriver)
}
