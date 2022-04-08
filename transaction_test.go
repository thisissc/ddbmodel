package ddbmodel

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/thisissc/awsclient"
	"github.com/thisissc/config"
)

//RUN the test need config.toml and dynamedb_table
const (
	TestDynamodbTableName = "Task"
)

type TestDate struct {
	Date  string `dynamodbav:",omitempty"`
	Count int    `dynamodbav:",omitempty"`
}

func SetupTest() {
	configFile := "config.toml"
	config.SetConfigFile(configFile)
	err := config.LoadConfig("AWS", &awsclient.Config{})
	if err != nil {
		log.Panic(err)
		return
	}
}

func generateWorkers(session *session.Session, count int) (workers []Worker) {
	if session == nil {
		err := errors.New("session is null")
		log.Panic(err)
	}
	for i := 0; i < count; i++ {
		workers = append(workers, *NewWorker(session, TestDynamodbTableName))
	}
	return
}

func generateSuccessCase(workers []Worker) (items []*dynamodb.Update) {
	actions := make([]map[string]interface{}, 0)
	actions = append(actions, map[string]interface{}{
		"Set": map[string]interface{}{
			"TaskName": "new_name",
		},
	})
	actions = append(actions, map[string]interface{}{
		"Add": map[string]interface{}{
			"Amount": 2,
		},
	})
	actions = append(actions, map[string]interface{}{
		"Set": map[string]interface{}{
			"CountArray": TestDate{
				Date:  "2022-8-2",
				Count: 3,
			},
		},
	})

	for i := range actions {
		worker := *workers[i].Key("ID", fmt.Sprint(i))
		for actionName, actionData := range actions[i] {
			item, err := worker.ToUpdateItem(actionName, actionData.((map[string]interface{})))
			if err != nil {
				log.Panic(err)
				return
			}
			items = append(items, &item)
		}
	}
	return
}

func TestTransaction_Transacte(t *testing.T) {
	type fields struct {
		AwsSession  *session.Session
		Workers     []Worker
		UpdateItems []*dynamodb.Update
	}
	SetupTest()
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "if the transcation have not error, should return success",
			fields: fields{
				AwsSession: awsclient.GetSession(),
				Workers:    generateWorkers(awsclient.GetSession(), 3),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := Transaction{
				AwsSession:  tt.fields.AwsSession,
				UpdateItems: generateSuccessCase(tt.fields.Workers),
			}
			if err := tr.Transacte(); (err != nil) != tt.wantErr {
				t.Errorf("Transaction.Transacte() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTransaction_TransacteWithConcurrency(t *testing.T) {
	type fields struct {
		AwsSession  *session.Session
		Workers     []Worker
		UpdateItems []*dynamodb.Update
	}
	SetupTest()
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "if the transcation with concurrency, should return error(TransactionConflict)",
			fields: fields{
				AwsSession: awsclient.GetSession(),
				Workers:    generateWorkers(awsclient.GetSession(), 3),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := Transaction{
				AwsSession:  tt.fields.AwsSession,
				UpdateItems: generateSuccessCase(tt.fields.Workers),
			}
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				tr.AwsSession = awsclient.GetSession()
				if err := tr.Transacte(); (err != nil) != tt.wantErr {
					t.Errorf("Transaction.Transacte() error = %v, wantErr %v", err, tt.wantErr)
				}
				wg.Done()
			}()
			go func() {
				tr.AwsSession = awsclient.GetSession()
				if err := tr.Transacte(); (err != nil) != tt.wantErr {
					t.Errorf("Transaction.Transacte() error = %v, wantErr %v", err, tt.wantErr)
				}
				wg.Done()
			}()
			wg.Wait()
		})
	}
}
