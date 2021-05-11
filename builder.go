package ddbmodel

import "github.com/aws/aws-sdk-go/service/dynamodb/expression"

func GenKeyConditionBuilder(data map[string]interface{}) expression.KeyConditionBuilder {
	firstKey := true
	var keyCond expression.KeyConditionBuilder
	for k, v := range data {
		kcb := expression.Key(k).Equal(expression.Value(v))
		if firstKey {
			keyCond = kcb
			firstKey = false
		} else {
			keyCond = keyCond.And(kcb)
		}
	}

	return keyCond
}

func GenConditionBuilder(data map[string]interface{}) expression.ConditionBuilder {
	firstKey := true
	var condBuilder expression.ConditionBuilder
	for k, v := range data {
		cb := expression.Name(k).Equal(expression.Value(v))
		if firstKey {
			condBuilder = cb
			firstKey = false
		} else {
			condBuilder = condBuilder.And(cb)
		}
	}

	return condBuilder
}
