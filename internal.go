package dynago

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"reflect"
)

// Internal values for dynamo sdk usage.
// These are lazily loaded singletons.
var (
	dynamoDbClient  *dynamodb.Client
	dynamoDbOptions *dynamodb.Options
	dynamoDbContext context.Context
)

func getClient(options dynamodb.Options) *dynamodb.Client {
	if dynamoDbClient == nil {
		dynamoDbClient = dynamodb.New(options)
	} else if !reflect.DeepEqual(*dynamoDbOptions, options) {
		dynamoDbOptions = &options
		dynamoDbClient = dynamodb.New(options)
	}

	return dynamoDbClient
}

func getContext() context.Context {
	if dynamoDbContext == nil {
		dynamoDbContext = context.Background()
	}

	return dynamoDbContext
}
