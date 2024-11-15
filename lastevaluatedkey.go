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

	newData := lastKeyMap(data)
	esKeyByte, _ := json.Marshal(newData)

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

type lastKey struct {
	B    []byte              `json:"B,omitempty"`
	BOOL *bool               `json:"BOOL,omitempty"`
	BS   [][]byte            `json:"BS,omitempty"`
	L    []*lastKey          `json:"L,omitempty"`
	M    map[string]*lastKey `json:"M,omitempty"`
	N    *string             `json:"N,omitempty"`
	NS   []*string           `json:"NS,omitempty"`
	NULL *bool               `json:"NULL,omitempty"`
	S    *string             `json:"S,omitempty"`
	SS   []*string           `json:"SS,omitempty"`
}

func newLastKey(attrVal *dynamodb.AttributeValue) *lastKey {
	newVal := lastKey{
		B:    attrVal.B,
		BOOL: attrVal.BOOL,
		BS:   attrVal.BS,
		N:    attrVal.N,
		NS:   attrVal.NS,
		NULL: attrVal.NULL,
		S:    attrVal.S,
		SS:   attrVal.SS,
	}

	if attrVal.L != nil {
		valList := make([]*lastKey, len(attrVal.L))
		for i, val := range attrVal.L {
			valList[i] = newLastKey(val)
		}
		newVal.L = valList
	}

	if attrVal.M != nil {
		valMap := make(map[string]*lastKey)
		for key, val := range attrVal.M {
			valMap[key] = newLastKey(val)
		}
		newVal.M = valMap
	}

	return &newVal
}

func lastKeyMap(data map[string]*dynamodb.AttributeValue) map[string]*lastKey {
	newData := make(map[string]*lastKey)
	for key, val := range data {
		newData[key] = newLastKey(val)
	}
	return newData
}
