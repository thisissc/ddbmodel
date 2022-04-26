package uglymodel

import (
	ddbmodel "github.com/thisissc/ddbmodel/v2"
)

type UglyModel struct {
	ddbmodel.Base

	ID        string `json:"id" dynamodbav:",omitempty"`
	UglyGroup string `json:"-" dynamodbav:",omitempty"`
	UglyId    string `json:"-" dynamodbav:",omitempty"`
}
