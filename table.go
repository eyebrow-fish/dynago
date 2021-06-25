package dynago

import (
	"context"
	"errors"
	"fmt"
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

	schemaValue := reflect.ValueOf(schema)
	schemaLength := schemaValue.NumField()

	if schemaLength < 1 {
		return nil, errors.New("expected at least one attribute in schema")
	}

	var attributes []types.AttributeDefinition
	for i := 0; i < schemaLength; i++ {
		field := schemaValue.Field(i)
		fieldType := field.Type()
		fieldName := fieldType.Name()

		attributeType, err := getAttributeType(field.Interface())
		if err != nil {
			return nil, err
		}

		fieldAttribute := types.AttributeDefinition{
			AttributeName: &fieldName,
			AttributeType: attributeType,
		}

		attributes = append(attributes, fieldAttribute)
	}

	var provision int64 = 1

	//goland:noinspection GoNilness
	output, err := client.CreateTable(getContext(), &dynamodb.CreateTableInput{
		TableName:            &name,
		AttributeDefinitions: attributes,
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: attributes[0].AttributeName,
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  &provision,
			WriteCapacityUnits: &provision,
		},
	})

	if err != nil {
		return nil, err
	}

	return &Table{*output.TableDescription.TableName}, nil
}

func getAttributeType(value interface{}) (types.ScalarAttributeType, error) {
	switch value.(type) {
	case string:
		return types.ScalarAttributeTypeS, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128:
		return types.ScalarAttributeTypeN, nil
	case bool:
		return "BOOL", nil
	default:
		return "", fmt.Errorf("unsupported type: %v", reflect.TypeOf(value))
	}
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
