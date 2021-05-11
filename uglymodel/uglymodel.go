package uglymodel

import (
	"crypto/md5"
	"fmt"

	"github.com/pkg/errors"
	"github.com/thisissc/awsclient"
	"github.com/thisissc/ddbmodel"
)

const (
	TableName      = "UglyModel"
	GroupIndexName = "UglyGroup-UglyId-index"
)

type UglyModel struct {
	ddbmodel.Base

	ID        string `json:"id" dynamodbav:",omitempty"`
	UglyGroup string `json:"-" dynamodbav:",omitempty"`
	UglyId    string `json:"-" dynamodbav:",omitempty"`
}

func Save(item interface{}) error {
	sess := awsclient.GetSession()
	dmw := ddbmodel.NewWorker(sess, TableName)

	return dmw.Save(item)
}

func RemoveItem(id string) error {
	sess := awsclient.GetSession()
	dmw := ddbmodel.NewWorker(sess, TableName).Key("ID", id)
	return dmw.Delete()
}

func FetchItem(id string, item interface{}) error {
	sess := awsclient.GetSession()
	dmw := ddbmodel.NewWorker(sess, TableName).Key("ID", id)

	return dmw.Get(item)
}

func FetchItemList(groupName string, itemList interface{}) error {
	sess := awsclient.GetSession()
	dmw := ddbmodel.NewWorker(sess, TableName).
		Keys(map[string]interface{}{
			"UglyGroup": groupName,
		}).
		Index(GroupIndexName)

	_, err := dmw.Query(itemList)
	return err
}

func FetchItemListById(groupName, uglyid string, itemList interface{}) error {
	sess := awsclient.GetSession()
	dmw := ddbmodel.NewWorker(sess, TableName)
	dmw.Keys(map[string]interface{}{
		"UglyGroup": groupName,
		"UglyId":    uglyid,
	}).Index(GroupIndexName)

	_, err := dmw.Query(itemList)
	if err != nil {
		return errors.Wrap(err, "FetchItemListById error")
	}

	return nil
}

func GenID(uglyGroup, uglyId string) string {
	baseStr := fmt.Sprintf("%s:%s", uglyGroup, uglyId)
	return fmt.Sprintf("%x", md5.Sum([]byte(baseStr)))
}
