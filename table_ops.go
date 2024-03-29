package dynago

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
)

// CreateTable attempts to create a DynamoDb table with the provided
// name and schema. The schema parameter accepts an "interface" as a value
// and it's field become the fields for the DynamoDb table.
// Example:
//
//  type Person struct {
//    Id  string
//	  Age int
//  }
//
//  dynago.CreateTable("TestTable", Person{})
//
// The created table exposes various DynamoDb API calls such as
// Table.Query and Table.Put.
func CreateTable(name string, schema interface{}) (*Table, error) {
	schemaValue := reflect.ValueOf(schema)
	schemaType := reflect.TypeOf(schema)
	schemaLength := schemaValue.NumField()

	if schemaLength < 1 {
		return nil, errors.New("expected at least one attribute in schema")
	}

	var attributes []types.AttributeDefinition
	for i := 0; i < schemaLength; i++ {
		fieldName := schemaType.Field(i).Name

		attributeType, err := toAttributeType(schemaValue.Field(i).Interface())
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

	return &Table{*output.TableDescription.TableName, schema, buildProjection(schema)}, nil
}

// ListTables is a simple operation which returns the list of
// all table names that are available to you.
func ListTables() ([]string, error) {
	output, err := dbClient.ListTables(dbCtx, &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %v", err)
	}

	return output.TableNames, nil
}
