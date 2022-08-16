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

package salesforce

import (
	"strconv"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/miquido/conduit-connector-azure-storage/source"
)

func Specification() sdk.Specification {
	return sdk.Specification{
		Name:              "azure-storage",
		Summary:           "An Azure Storage source plugin for Conduit.",
		Description:       "The Conduit plugin supporting Azure Storage source.",
		Version:           "v0.1.0",
		Author:            "Miquido",
		DestinationParams: map[string]sdk.Parameter{
			//
		},
		SourceParams: map[string]sdk.Parameter{
			source.ConfigKeyConnectionString: {
				Default:     "",
				Required:    true,
				Description: "The Azure Storage connection string.",
			},
			source.ConfigKeyContainerName: {
				Default:     "",
				Required:    true,
				Description: "The name of the container to monitor.",
			},
			source.ConfigKeyPollingPeriod: {
				Default:     source.DefaultPollingPeriod,
				Required:    false,
				Description: "The polling period for the CDC mode, formatted as a time.Duration string.",
			},
			source.ConfigKeyMaxResults: {
				Default:     strconv.FormatInt(int64(source.DefaultMaxResults), 32),
				Required:    false,
				Description: "The maximum number of items, per page, when reading container's items.",
			},
		},
	}
}
