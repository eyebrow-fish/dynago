package dynago

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Table struct {
	name string
}

func NewTable(name string) (*Table, error) {
	var err error
	sess, err = session.NewSession(aws.NewConfig())
	if err != nil {
		return nil, err
	}
	dynamo = dynamodb.New(sess)
	return &Table{name}, nil
}

var (
	sess *session.Session
	dynamo *dynamodb.DynamoDB
)
