package ddbmodel

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

type Worker struct {
	Client           *dynamodb.Client
	TableName        string
	IndexName        string
	InputKey         map[string]interface{}
	InputFilter      map[string]interface{}
	QueryOffset      string
	ReverseOrder     bool
	QueryLimit       int32
	IsConsistentRead bool
	ProjectionAttrs  []string
}

func NewWorker(client *dynamodb.Client) *Worker {
	return &Worker{
		Client: client,
	}
}

func (w *Worker) Table(name string) *Worker {
	w.TableName = name
	return w
}

func (w *Worker) Index(indexName string) *Worker {
	w.IndexName = indexName
	return w
}

func (w *Worker) Offset(offset string) *Worker {
	w.QueryOffset = offset
	return w
}

func (w *Worker) Limit(limit int32) *Worker {
	w.QueryLimit = limit
	return w
}

func (w *Worker) Reverse(reverseOrder bool) *Worker {
	w.ReverseOrder = reverseOrder
	return w
}

func (w *Worker) ConsistentRead(isConsistentRead bool) *Worker {
	w.IsConsistentRead = isConsistentRead
	return w
}

func (w *Worker) Projection(attrs []string) *Worker {
	w.ProjectionAttrs = attrs
	return w
}

func (w *Worker) Filter(key string, value interface{}) *Worker {
	if w.InputFilter == nil {
		w.InputFilter = make(map[string]interface{}, 0)
	}

	w.InputFilter[key] = value

	return w
}

func (w *Worker) Key(key string, value interface{}) *Worker {
	if w.InputKey == nil {
		w.InputKey = make(map[string]interface{}, 0)
	}

	w.InputKey[key] = value

	return w
}

func (w *Worker) Keys(params map[string]interface{}) *Worker {
	for k, v := range params {
		w.Key(k, v)
	}
	return w
}

func (w *Worker) Save(obj interface{}) error {
	av, err := attributevalue.MarshalMap(obj)
	if err != nil {
		return errors.Wrap(err, "attributevalue marshal failed")
	}

	_, err = w.Client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(w.TableName),
		Item:      av,
	})

	if err != nil {
		return errors.Wrap(err, "dynamodb put item failed")
	}

	return nil
}

// FIXME:
// A single call to BatchWriteItem can write up to 16 MB of data, which can comprise as many as 25 put or delete requests. Individual items to be written can be as large as 400 KB.
func (w *Worker) BatchSave(items []interface{}) error {
	writeRequestList := make([]types.WriteRequest, len(items))
	for i, obj := range items {
		av, _ := attributevalue.MarshalMap(obj)
		wr := types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: av,
			},
		}
		writeRequestList[i] = wr
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			w.TableName: writeRequestList,
		},
	}

	_, err := w.Client.BatchWriteItem(context.TODO(), input)
	if err != nil {
		return errors.Wrap(err, "dynamodb BatchWriteItem failed")
	}

	return nil
}

func (w *Worker) Delete() error {
	key, err := attributevalue.MarshalMap(w.InputKey)
	if err != nil {
		return errors.Wrap(err, "MarshalMap error")
	}

	input := &dynamodb.DeleteItemInput{
		Key:       key,
		TableName: aws.String(w.TableName),
	}

	_, err = w.Client.DeleteItem(context.TODO(), input)
	if err != nil {
		return errors.Wrap(err, "Delete item error")
	}

	return nil
}

func (w *Worker) Get(dst interface{}) error {
	key, err := attributevalue.MarshalMap(w.InputKey)
	if err != nil {
		return errors.Wrap(err, "MarshalMap error")
	}

	input := &dynamodb.GetItemInput{
		Key:            key,
		TableName:      aws.String(w.TableName),
		ConsistentRead: aws.Bool(w.IsConsistentRead),
	}

	result, err := w.Client.GetItem(context.TODO(), input)
	if err != nil {
		return errors.Wrap(err, "Get item error")
	}

	if len(result.Item) > 0 {
		err = attributevalue.UnmarshalMap(result.Item, dst)
		if err != nil {
			return errors.Wrap(err, "Unmarshal item error")
		}
	} else {
		return &DdbModelEmptyError{}
	}

	return nil
}

// FIXME:
// A single operation can retrieve up to 16 MB of data, which can contain as many as 100 items.
func (w *Worker) BatchGet(pkName string, ids []string, itemList interface{}) error {
	keys := make([]map[string]types.AttributeValue, len(ids))
	for i, id := range ids {
		curKey := map[string]types.AttributeValue{
			pkName: &types.AttributeValueMemberS{
				Value: id,
			},
		}
		keys[i] = curKey
	}

	keysAndAttrs := types.KeysAndAttributes{
		Keys: keys,
	}

	projLen := len(w.ProjectionAttrs)
	if projLen > 0 {
		expAttrNames := make([]string, projLen)
		expAttrNameMap := make(map[string]string, projLen)
		for i, name := range w.ProjectionAttrs {
			aliasName := fmt.Sprintf("#EAN%d", i)
			expAttrNames[i] = aliasName
			expAttrNameMap[aliasName] = name
		}
		keysAndAttrs.ExpressionAttributeNames = expAttrNameMap
		keysAndAttrs.ProjectionExpression = aws.String(strings.Join(expAttrNames, ","))
	}

	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			w.TableName: keysAndAttrs,
		},
	}

	resp, err := w.Client.BatchGetItem(context.TODO(), input)

	if err != nil {
		log.Println(err)
		return errors.Wrap(err, "Client failed")
	}

	if values, ok := resp.Responses[w.TableName]; ok {
		err = attributevalue.UnmarshalListOfMaps(values, itemList)

		if err != nil {
			return errors.Wrap(err, "UnmarshalListOfMaps failed")
		}
	}
	return nil
}

func (w *Worker) Query(itemList interface{}) (string, error) {
	builder := expression.NewBuilder()

	if len(w.InputKey) > 0 {
		keyCond := GenKeyConditionBuilder(w.InputKey)
		builder = builder.WithKeyCondition(keyCond)
	}

	projLen := len(w.ProjectionAttrs)
	if projLen > 0 {
		projNameBuilders := make([]expression.NameBuilder, projLen)

		for i, n := range w.ProjectionAttrs {
			projNameBuilders[i] = expression.Name(n)
		}

		proj := expression.ProjectionBuilder{}.AddNames(projNameBuilders...)
		builder = builder.WithProjection(proj)
	}

	if len(w.InputFilter) > 0 {
		condBuilder := GenConditionBuilder(w.InputFilter)
		builder = builder.WithFilter(condBuilder)
	}

	expr, err := builder.Build()
	if err != nil {
		return "", errors.Wrap(err, "Build expression error")
	}

	return w.QueryByExpression(expr, itemList)
}

func (w *Worker) QueryByExpression(expr expression.Expression, itemList interface{}) (string, error) {
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(w.TableName),
	}

	if len(w.IndexName) > 0 {
		input.IndexName = aws.String(w.IndexName)
	}

	if len(w.QueryOffset) > 0 {
		esKey := DecodeLastEvaluatedKey(w.QueryOffset)
		if esKey != nil {
			input.ExclusiveStartKey = esKey
		}
	}

	if w.ReverseOrder {
		input.ScanIndexForward = aws.Bool(false)
	}

	if w.QueryLimit > 0 {
		input.Limit = aws.Int32(w.QueryLimit)
	}

	offset := ""

	result, err := w.Client.Query(context.TODO(), input)
	if err != nil {
		return offset, errors.Wrap(err, "Query item list failed")
	}

	if len(result.Items) > 0 {
		err = attributevalue.UnmarshalListOfMaps(result.Items, itemList)
		if err != nil {
			return offset, errors.Wrap(err, "UnmarshalListOfMaps failed")
		} else {
			offset = EncodeLastEvaluatedKey(result.LastEvaluatedKey)
			w.QueryOffset = offset
		}
	}

	return offset, nil
}

func (w *Worker) Scan(itemList interface{}) (string, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(w.TableName),
	}

	builderSeted := false

	builder := expression.NewBuilder()

	projLen := len(w.ProjectionAttrs)
	if projLen > 0 {
		projNameBuilders := make([]expression.NameBuilder, projLen)

		for i, n := range w.ProjectionAttrs {
			projNameBuilders[i] = expression.Name(n)
		}

		proj := expression.ProjectionBuilder{}.AddNames(projNameBuilders...)
		builder = builder.WithProjection(proj)

		builderSeted = true
	}

	if len(w.InputFilter) > 0 {
		condBuilder := GenConditionBuilder(w.InputFilter)
		builder = builder.WithFilter(condBuilder)

		builderSeted = true
	}

	if builderSeted {
		expr, err := builder.Build()
		if err != nil {
			return "", errors.Wrap(err, "Build expression error")
		}

		input.ExpressionAttributeNames = expr.Names()
		input.ExpressionAttributeValues = expr.Values()
		input.FilterExpression = expr.Filter()
		input.ProjectionExpression = expr.Projection()
	}

	if len(w.IndexName) > 0 {
		input.IndexName = aws.String(w.IndexName)
	}

	if len(w.QueryOffset) > 0 {
		esKey := DecodeLastEvaluatedKey(w.QueryOffset)
		if esKey != nil {
			input.ExclusiveStartKey = esKey
		}
	}

	if w.QueryLimit > 0 {
		input.Limit = aws.Int32(w.QueryLimit)
	}

	offset := ""

	result, err := w.Client.Scan(context.TODO(), input)
	if err != nil {
		return offset, errors.Wrap(err, "Scan item list failed")
	}

	if len(result.Items) > 0 {
		err = attributevalue.UnmarshalListOfMaps(result.Items, itemList)
		if err != nil {
			return offset, errors.Wrap(err, "UnmarshalListOfMaps failed")
		} else {
			offset = EncodeLastEvaluatedKey(result.LastEvaluatedKey)
			w.QueryOffset = offset
		}
	}

	return offset, nil
}

func (w *Worker) Incr(key string, increment int64) error {
	update := expression.Add(
		expression.Name(key),
		expression.Value(increment),
	)

	expr, _ := expression.NewBuilder().
		WithUpdate(update).
		Build()

	return w.UpdateByExpression(expr)
}

func (w *Worker) Add2Set(key string, values []string) error {
	update := expression.Add(
		expression.Name(key),
		expression.Value(&types.AttributeValueMemberSS{
			Value: values,
		}),
	)

	expr, _ := expression.NewBuilder().
		WithUpdate(update).
		Build()

	return w.UpdateByExpression(expr)
}

func (w *Worker) Update(key string, value interface{}) error {
	update := expression.Set(
		expression.Name(key),
		expression.Value(value),
	)

	expr, _ := expression.NewBuilder().
		WithUpdate(update).
		Build()

	return w.UpdateByExpression(expr)
}

func (w *Worker) RemoveAttribute(key string) error {
	remove := expression.Remove(
		expression.Name(key),
	)

	expr, _ := expression.NewBuilder().
		WithUpdate(remove).
		Build()

	return w.UpdateByExpression(expr)
}

func (w *Worker) BatchUpdate(data map[string]interface{}) error {
	update := expression.UpdateBuilder{}
	for k, v := range data {
		update = update.Set(
			expression.Name(k),
			expression.Value(v),
		)
	}

	expr, _ := expression.NewBuilder().
		WithUpdate(update).
		Build()

	return w.UpdateByExpression(expr)
}

func (w *Worker) UpdateByExpression(expr expression.Expression) error {
	key, err := attributevalue.MarshalMap(w.InputKey)
	if err != nil {
		return errors.Wrap(err, "MarshalMap error")
	}

	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		Key:                       key,
		ReturnValues:              types.ReturnValueNone,
		TableName:                 aws.String(w.TableName),
	}

	_, err = w.Client.UpdateItem(context.TODO(), input)
	if err != nil {
		return errors.Wrap(err, "Query item list failed")
	}

	return nil
}
