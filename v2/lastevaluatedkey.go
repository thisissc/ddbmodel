package ddbmodel

import (
	"encoding/base64"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func EncodeLastEvaluatedKey(data map[string]types.AttributeValue) string {
	if len(data) == 0 {
		return ""
	}

	var mapData map[string]interface{}
	attributevalue.UnmarshalMap(data, &mapData)
	esKeyByte, _ := json.Marshal(mapData)
	encoded := base64.StdEncoding.EncodeToString(esKeyByte)
	return encoded
}

func DecodeLastEvaluatedKey(data string) map[string]types.AttributeValue {
	decodedData, err := base64.StdEncoding.DecodeString(data)
	if err == nil {
		var esKey map[string]types.AttributeValue
		var mapData map[string]interface{}
		err = json.Unmarshal([]byte(decodedData), &mapData)
		if err == nil {
			esKey, err = attributevalue.MarshalMap(mapData)
			if err == nil {
				return esKey
			}
		}
	}

	return nil
}
