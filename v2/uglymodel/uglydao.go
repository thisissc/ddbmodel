package uglymodel

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pkg/errors"
	ddbmodel "github.com/thisissc/ddbmodel/v2"
)

const (
	TableName      = "UglyModel"
	GroupIndexName = "UglyGroup-UglyId-index"
)

type FindInput struct {
	UglyGroup string
	UglyId    string
}

type queryInput struct {
	UglyId           string
	UglyIdStartsWith string
	Offset           string
	Reverse          bool
}

type UglyDao struct {
	ctx    context.Context
	Client *dynamodb.Client

	queryInput queryInput
}

func NewDao(ctx context.Context, client *dynamodb.Client) UglyDao {
	return UglyDao{
		ctx:    ctx,
		Client: client,
	}
}

func (dao *UglyDao) Save(item interface{}) error {
	dmw := ddbmodel.NewWorker(dao.ctx, dao.Client).
		Table(TableName)

	return dmw.Save(item)
}

func (dao *UglyDao) Delete(id string) error {
	dmw := ddbmodel.NewWorker(dao.ctx, dao.Client).
		Table(TableName).
		Key("ID", id)

	return dmw.Delete()
}

func (dao *UglyDao) Get(id string, item interface{}) error {
	dmw := ddbmodel.NewWorker(dao.ctx, dao.Client).
		Table(TableName).
		Key("ID", id)

	return dmw.Get(item)
}

// FIXME: deprecated
func (dao *UglyDao) Find(input FindInput, itemList interface{}) error {
	params := map[string]interface{}{}
	if len(input.UglyGroup) > 0 {
		params["UglyGroup"] = input.UglyGroup
	}

	if len(input.UglyId) > 0 {
		params["UglyId"] = input.UglyId
	}

	dmw := ddbmodel.NewWorker(dao.ctx, dao.Client).
		Table(TableName).
		Index(GroupIndexName).
		Keys(params)

	_, err := dmw.Query(itemList)
	if err != nil {
		return errors.Wrap(err, "Find error")
	}

	return nil
}

func (dao *UglyDao) Query(group string, itemList interface{}, opts ...func(*UglyDao)) (string, error) {
	for _, opt := range opts {
		opt(dao)
	}

	input := dao.queryInput

	kcb := expression.Key("UglyGroup").Equal(expression.Value(group))

	if len(dao.queryInput.UglyId) > 0 {
		kcb2 := expression.Key("UglyId").Equal(expression.Value(input.UglyId))
		kcb = kcb.And(kcb2)
	} else if len(dao.queryInput.UglyIdStartsWith) > 0 {
		kcb2 := expression.Key("UglyId").BeginsWith(input.UglyIdStartsWith)
		kcb = kcb.And(kcb2)
	}

	builder := expression.NewBuilder()
	builder = builder.WithKeyCondition(kcb)

	expr, err := builder.Build()
	if err != nil {
		return "", err
	}

	dmw := ddbmodel.NewWorker(dao.ctx, dao.Client).
		Table(TableName).
		Index(GroupIndexName).
		Offset(input.Offset).
		Reverse(input.Reverse)

	offset, err := dmw.QueryByExpression(expr, &itemList)
	if err != nil {
		return "", errors.Wrap(err, "Find error")
	}

	return offset, nil
}

func UglyId(s string) func(*UglyDao) {
	return func(dao *UglyDao) {
		dao.queryInput.UglyId = s
	}
}

func UglyIdStartsWith(s string) func(*UglyDao) {
	return func(dao *UglyDao) {
		dao.queryInput.UglyIdStartsWith = s
	}
}

func Offset(s string) func(*UglyDao) {
	return func(dao *UglyDao) {
		dao.queryInput.Offset = s
	}
}

func Reverse(b bool) func(*UglyDao) {
	return func(dao *UglyDao) {
		dao.queryInput.Reverse = b
	}
}
