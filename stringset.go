package ddbmodel

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type StringSet []string

func (ss StringSet) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	av.SS = make([]*string, 0, len(ss))
	for _, v := range ss {
		av.SS = append(av.SS, aws.String(v))
	}
	return nil
}
