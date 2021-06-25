package dynago

import (
	"errors"
	"fmt"
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

func getAttributeType(value interface{}) (types.ScalarAttributeType, error) {
	switch value.(type) {
	case string:
		return "S", nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128:
		return "N", nil
	case bool:
		return "BOOL", nil
	default:
		return "", fmt.Errorf("unsupported type: %v", reflect.TypeOf(value))
	}
}
