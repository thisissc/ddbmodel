package uglymodel

import (
	"context"

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

type UglyDao struct {
	ctx    context.Context
	Client *dynamodb.Client
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
