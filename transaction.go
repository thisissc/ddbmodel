package ddbmodel

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
)

type Transaction struct {
	AwsSession  *session.Session
	UpdateItems []*dynamodb.Update
}

func NewTransaction(sess *session.Session, items []*dynamodb.Update) Transaction {
	return Transaction{
		AwsSession:  sess,
		UpdateItems: items,
	}
}

func (t Transaction) Transacte() error {
	items := make([]*dynamodb.TransactWriteItem, 0)
	for i := range t.UpdateItems {
		items = append(items, &dynamodb.TransactWriteItem{
			Update: t.UpdateItems[i],
		})
	}
	_, err := dynamodb.New(t.AwsSession).TransactWriteItems(
		&dynamodb.TransactWriteItemsInput{
			TransactItems: items,
		})
	if err != nil {
		return errors.Wrap(err, "transaction failed")
	}
	return nil
}
