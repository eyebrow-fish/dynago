package dynago

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
)

type Table struct {
	Name   string
	Schema interface{}
}

func NewTable(name string, schema interface{}) (*Table, error) {
	output, err := dbClient.DescribeTable(dbCtx, &dynamodb.DescribeTableInput{TableName: &name})
	if err != nil {
		return nil, err
	}

	return &Table{*output.Table.TableName, schema}, nil
}

func (t Table) QueryWithExpr(expr string, values map[string]interface{}) ([]interface{}, error) {
	attributeValues := make(map[string]types.AttributeValue)
	for k, v := range values {
		value, err := toAttributeValue(v)
		if err != nil {
			return nil, err
		}

		attributeValues[k] = value
	}

	output, err := dbClient.Query(dbCtx, &dynamodb.QueryInput{
		TableName:                 &t.Name,
		ExpressionAttributeValues: attributeValues,
		KeyConditionExpression:    &expr,
	})

	if err != nil {
		return nil, err
	}

	return buildItems(output.Items, t.Schema)
}

func CreateTable(name string, schema interface{}) (*Table, error) {
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

		attributeType, err := toAttributeType(field.Interface())
		if err != nil {
			return nil, err
		}

		fieldAttribute := types.AttributeDefinition{
			AttributeName: &fieldName,
			AttributeType: attributeType,
		}

		attributes = append(attributes, fieldAttribute)
	}

	var keySchema []types.KeySchemaElement

	//goland:noinspection GoNilness
	keySchema = append(keySchema, types.KeySchemaElement{
		AttributeName: attributes[0].AttributeName,
		KeyType:       types.KeyTypeHash,
	})

	//goland:noinspection GoNilness
	if len(attributes) > 1 {
		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: attributes[1].AttributeName,
			KeyType:       types.KeyTypeRange,
		})
	}

	var provision int64 = 1

	output, err := dbClient.CreateTable(dbCtx, &dynamodb.CreateTableInput{
		TableName:            &name,
		AttributeDefinitions: attributes,
		KeySchema:            keySchema,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  &provision,
			WriteCapacityUnits: &provision,
		},
	})

	if err != nil {
		return nil, err
	}

	return &Table{*output.TableDescription.TableName, schema}, nil
}
