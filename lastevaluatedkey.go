package ddbmodel

import (
	"encoding/base64"
	"encoding/json"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func EncodeLastEvaluatedKey(data map[string]*dynamodb.AttributeValue) string {
	if len(data) == 0 {
		return ""
	}

	esKeyByte, _ := json.Marshal(data)
	encoded := base64.StdEncoding.EncodeToString(esKeyByte)
	return encoded
}

func DecodeLastEvaluatedKey(data string) map[string]*dynamodb.AttributeValue {
	decodedData, err := base64.StdEncoding.DecodeString(data)
	if err == nil {
		var esKey map[string]*dynamodb.AttributeValue
		err = json.Unmarshal([]byte(decodedData), &esKey)
		if err == nil {
			return esKey
		}
	}

	return nil
}
