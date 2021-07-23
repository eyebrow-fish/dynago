package dynago

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Table struct {
	Name   string
	Schema interface{}
}

// NewTable creates a new Table.
// A Table cannot be created if the Table does not exist in DynamoDb.
func NewTable(name string, schema interface{}) (*Table, error) {
	output, err := dbClient.DescribeTable(dbCtx, &dynamodb.DescribeTableInput{TableName: &name})
	if err != nil {
		return nil, err
	}

	return &Table{*output.Table.TableName, schema}, nil
}

func (t Table) Query(condition Condition) ([]interface{}, error) {
	expr, values := condition.buildExpr()
	return t.QueryWithExpr(*expr, values, condition.options.limit)
}

func (t Table) QueryWithExpr(expr string, values map[string]interface{}, limit *int32) ([]interface{}, error) {
	output, err := dbClient.Query(dbCtx, &dynamodb.QueryInput{
		TableName:                 &t.Name,
		ExpressionAttributeValues: fromMap(values),
		KeyConditionExpression:    &expr,
		Limit:                     limit,
	})

	if err != nil {
		return nil, err
	}

	return constructItems(output.Items, t.Schema)
}

func (t Table) ScanAll() ([]interface{}, error) { return t.Scan(All()) }

func (t Table) Scan(condition Condition) ([]interface{}, error) {
	expr, values := condition.buildExpr()

	output, err := dbClient.Scan(dbCtx, &dynamodb.ScanInput{
		TableName:                 &t.Name,
		ExpressionAttributeValues: fromMap(values),
		FilterExpression:          expr,
		Limit:                     condition.options.limit,
	})

	if err != nil {
		return nil, err
	}

	return constructItems(output.Items, t.Schema)
}

func (t Table) Put(item interface{}) (interface{}, error) { return t.PutWithCondition(All(), item) }

func (t Table) PutWithCondition(condition Condition, item interface{}) (interface{}, error) {
	toPut := buildItem(item)
	expr, values := condition.buildExpr()

	_, err := dbClient.PutItem(dbCtx, &dynamodb.PutItemInput{
		TableName:                 &t.Name,
		Item:                      toPut,
		ExpressionAttributeValues: fromMap(values),
		ConditionExpression:       expr,
	})

	return item, err
}
