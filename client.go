package dynago

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func UpdateOptions(options dynamodb.Options) {
	dbClient = dynamodb.New(options)
}

var (
	dbClient *dynamodb.Client
	dbCtx    context.Context
)

func init() {
	dbClient = dynamodb.New(dynamodb.Options{})
	dbCtx = context.Background()
}
