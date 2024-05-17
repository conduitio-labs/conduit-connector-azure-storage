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

//go:generate paramgen -output config_paramgen.go Config

package source

import (
	"time"
)

const (
	ConfigKeyConnectionString = "connectionString"
	ConfigKeyContainerName    = "containerName"

	ConfigKeyPollingPeriod = "pollingPeriod"
	ConfigKeyMaxResults    = "maxResults"
)

type Config struct {
	// The Azure Storage connection string.
	ConnectionString string `json:"connectionString" validate:"required"`
	// The name of the container to monitor.
	ContainerName string `json:"containerName" validate:"required"`
	// The polling period for the CDC mode, formatted as a time.Duration string.
	PollingPeriod time.Duration `json:"pollingPeriod" default:"1s"`
	// The maximum number of items, per page, when reading container's items.
	MaxResults int32 `json:"maxResults" default:"5000" validate:"gt=0,lt=5001"`
}
