package dynago

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
)

type Table struct {
	Name string
}

func NewTable(name string, schema interface{}) (*Table, error) {
	return nil, errors.New("unimplemented")
}

func CreateTable(name string, schema interface{}) (*Table, error) {
	return CreateTableWithOptions(name, schema, dynamodb.Options{})
}

func CreateTableWithOptions(name string, schema interface{}, options dynamodb.Options) (*Table, error) {
	client := getClient(options)

	output, err := client.CreateTable(getContext(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{},
		KeySchema:            []types.KeySchemaElement{},
		TableName:            &name,
	})

	if err != nil {
		return nil, err
	}

	return &Table{*output.TableDescription.TableName}, nil
}

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
