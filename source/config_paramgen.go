// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-connector-sdk/tree/main/cmd/paramgen

package source

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (Config) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"connectionString": {
			Default:     "",
			Description: "The Azure Storage connection string.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"containerName": {
			Default:     "",
			Description: "The name of the container to monitor.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"maxResults": {
			Default:     "5000",
			Description: "The maximum number of items, per page, when reading container's items.",
			Type:        sdk.ParameterTypeInt,
			Validations: []sdk.Validation{
				sdk.ValidationGreaterThan{Value: 0},
				sdk.ValidationLessThan{Value: 5001},
			},
		},
		"pollingPeriod": {
			Default:     "1s",
			Description: "The polling period for the CDC mode, formatted as a time.Duration string.",
			Type:        sdk.ParameterTypeDuration,
			Validations: []sdk.Validation{},
		},
	}
}
