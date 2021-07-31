package dynago

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Table provides all item operations for your DynamoDb Table.
// This includes queries, scans, puts, deletions, etc.
//
// The operations performed on this Table will result in an
// interface whose type is the same as the Schema field.
// This is true unless an error is returned instead.
type Table struct {
	Name   string
	Schema interface{}
}

// NewTable creates a new Table.
// A Table cannot be created if the Table does not exist in DynamoDb.
//
// The operations performed on this Table will result in an
// interface whose type is the same as the schema parameter.
// This is true unless an error is returned instead.
func NewTable(name string, schema interface{}) (*Table, error) {
	output, err := dbClient.DescribeTable(dbCtx, &dynamodb.DescribeTableInput{TableName: &name})
	if err != nil {
		return nil, err
	}

	return &Table{*output.Table.TableName, schema}, nil
}

// Query allows query operation access on the Table.
// A Condition is given as an argument for easy usage.
//
// The result of Query, in the case of no error being returned,
// will be the same type as your Table's schema.
//
//  // Don't forget to handle errors in your code!
//  result, err := table.Query(dynago.Eq("Id", dynago.N(123)))
//  data := result.(MySchema)
//
func (t Table) Query(condition Condition) ([]interface{}, error) {
	expr, values := condition.buildExpr()
	return t.QueryWithExpr(*expr, values, condition.options.limit)
}

// QueryWithExpr allows for lower level usage of your Table.
// No fancy Condition construction, just a map of strings to interfaces
// along with a query expression. A nillable limit is also required as a
// parameter, but it can be set to nil if no limit is needed.
//
// Query actually wraps this function but translates the Condition into
// the function parameters.
//
//  result, _ := table.QueryWithExpr("Id = :Id", map[string]interface{}{":Id": "123"}, nil)
func (t Table) QueryWithExpr(expr string, values map[string]interface{}, limit *int32) ([]interface{}, error) {
	var items []map[string]types.AttributeValue

	var doQuery func(lastKey map[string]types.AttributeValue) error
	doQuery = func(lastKey map[string]types.AttributeValue) error {
		output, err := dbClient.Query(dbCtx, &dynamodb.QueryInput{
			TableName:                 &t.Name,
			ExpressionAttributeValues: fromMap(values),
			KeyConditionExpression:    &expr,
			Limit:                     limit,
			ExclusiveStartKey:         lastKey,
		})

		if err != nil {
			return err
		}

		items = append(items, output.Items...)

		// No LastEvaluatedKey or the given limit is met
		if len(output.LastEvaluatedKey) > 0 && limit == nil || limit != nil && int32(len(items)) < *limit {
			return doQuery(output.LastEvaluatedKey)
		}

		return nil
	}

	if err := doQuery(nil); err != nil {
		return nil, err
	}

	return constructItems(items, t.Schema)
}

// ScanAll simply scans all items in your Table and returns them.
//
// Scan operations normally are not fast unless your data set is small.
// Do not use this on larger tables unless you know what you're doing.
func (t Table) ScanAll() ([]interface{}, error) { return t.Scan(All()) }

// Scan performs a basic item scan on your Table using the provided Condition.
//
// Scan operations normally are not fast unless your data set is small.
// Do not use this on larger tables unless you know what you're doing.
func (t Table) Scan(condition Condition) ([]interface{}, error) {
	expr, values := condition.buildExpr()
	limit := condition.options.limit

	var items []map[string]types.AttributeValue

	var doScan func(lastKey map[string]types.AttributeValue) error
	doScan = func(lastKey map[string]types.AttributeValue) error {
		output, err := dbClient.Scan(dbCtx, &dynamodb.ScanInput{
			TableName:                 &t.Name,
			ExpressionAttributeValues: fromMap(values),
			FilterExpression:          expr,
			Limit:                     limit,
			ExclusiveStartKey:         lastKey,
		})

		if err != nil {
			return err
		}

		items = append(items, output.Items...)

		// No LastEvaluatedKey or the given limit is met
		if len(output.LastEvaluatedKey) > 0 && limit == nil || limit != nil && int32(len(items)) < *limit {
			return doScan(output.LastEvaluatedKey)
		}

		return nil
	}

	if err := doScan(nil); err != nil {
		return nil, err
	}

	return constructItems(items, t.Schema)
}

// Put allows you to put an item into your Table.
// This function will do it with no questions asked, unless
// there was and underlying error.
//
// The returned value will be the put item, unless an error occurred.
//
//  put, err := dynago.Put(item)
func (t Table) Put(item interface{}) (interface{}, error) { return t.PutWithCondition(All(), item) }

// PutWithCondition behaves the same as Table.Put but it  accepts a
// Condition that must be met before putting the given item.
func (t Table) PutWithCondition(condition Condition, item interface{}) (interface{}, error) {
	toPut := buildItem(item)
	expr, values := condition.buildExpr()

	_, err := dbClient.PutItem(dbCtx, &dynamodb.PutItemInput{
		TableName:                 &t.Name,
		Item:                      toPut,
		ExpressionAttributeValues: fromMap(values),
		ConditionExpression:       expr,
	})

	if err != nil {
		return nil, err
	}

	return item, nil
}

// DeleteItem attempts to delete the item from your Table.
// Unless an error occurs, the returned value will be the item
// that was deleted.
//
// For a more powerful deletion checkout Delete.
func (t Table) DeleteItem(item interface{}) (interface{}, error) {
	toDelete := buildItem(item)

	_, err := dbClient.DeleteItem(dbCtx, &dynamodb.DeleteItemInput{
		TableName: &t.Name,
		Key:       toDelete,
	})

	if err != nil {
		return nil, err
	}

	return item, nil
}

// Delete accepts a Condition as a predicate to compare against items
// in the Table.
// The items returned are the ones which were deleted, unless an error
// occurred.
//
//  deleted, err := table.Delete(dynago.Bt("Age", dynago.N(0), dynago.N(17)))
//  // Much needed error handling
//  if len(deleted) >= 2_200_000_000 {
//    log.Fatalln("We just deleted all children!")
//	}
//
// Under the hood, a query is run with the given Condition and a bulk
// deletion is attempted.
func (t Table) Delete(condition Condition) (interface{}, error) {
	items, err := t.Query(condition)
	if err != nil {
		return nil, err
	}

	var writes []types.WriteRequest
	for _, item := range items {
		writes = append(writes, types.WriteRequest{DeleteRequest: &types.DeleteRequest{
			Key: buildItem(item),
		}})
	}

	_, err = dbClient.BatchWriteItem(dbCtx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{t.Name: writes},
	})

	if err != nil {
		return nil, err
	}

	return items, nil
}
