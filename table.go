package dynago

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

	return constructItems(output.Items, t.Schema)
}

func (t Table) Put(item interface{}) (interface{}, error) {
	toPut, err := buildItem(item)
	if err != nil {
		return nil, err
	}

	_, err = dbClient.PutItem(dbCtx, &dynamodb.PutItemInput{
		TableName: &t.Name,
		Item:      toPut,
	})

	return item, err
}
