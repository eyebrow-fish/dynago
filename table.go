package dynago

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

// A Table is a DynamoDb table client.
//
// A Table is the primary way to interact with DynamoDb
// and all queries and mutations are served through it.
//
// * Note that all tables initialized with the same
// aws.Config will reuse the same dynamodb.DynamoDB client.
type Table struct {
	name     string
	dataType interface{}
	conf     *aws.Config
}

// NewTable initializes a Table with the default
// aws.Config.
//
// The name parameter is the name of the table in AWS.
//
// The dataType parameter should contain an empty
// interface used for the queries and mutations of
// the table.
//
// See NewTableWithConfig for more configuration.
func NewTable(name string, dataType interface{}) (*Table, error) {
	return NewTableWithConfig(name, dataType, *aws.NewConfig())
}

// NewTableWithConfig allows custom a aws.Config to be
// passed in. Variables such as Region and Endpoint can be
// configured when using this constructor. Otherwise, the
// behavior is the same as NewTable.
//
// * Note that all tables initialized with the same
// aws.Config will reuse the same dynamodb.DynamoDB client.
func NewTableWithConfig(name string, dataType interface{}, conf aws.Config) (*Table, error) {
	var confKey *aws.Config
	for c, _ := range dynamoConf {
		if reflect.DeepEqual(*c, conf) {
			confKey = c
		}
	}
	if confKey == nil {
		sess, err := session.NewSession(&conf)
		if err != nil {
			return nil, err
		}
		dynamoConf[&conf] = dynamodb.New(sess)
		confKey = &conf
	}
	return &Table{name, dataType, confKey}, nil
}

// Query is a method which queries items on a Table.
//
// The response of Query will be items which matches the
// Cond(s) given. Additionally, the structure will match
// the dataType parameter given to the Table constructor.
func (t *Table) Query(cons ...Cond) ([]interface{}, error) {
	keyCons := make(map[string]*dynamodb.Condition)
	for _, v := range cons {
		val, err := v.val.attrVal()
		if err != nil {
			return nil, err
		}
		compOp, err := v.op.compOp()
		if v.op == n || v.op == nn {
			keyCons[v.key] = &dynamodb.Condition{
				ComparisonOperator: compOp,
			}
		} else {
			keyCons[v.key] = &dynamodb.Condition{
				AttributeValueList: []*dynamodb.AttributeValue{val},
				ComparisonOperator: compOp,
			}
		}
	}
	proj := projOf(t.dataType)
	q := dynamodb.QueryInput{
		TableName:            &t.name,
		KeyConditions:        keyCons,
		ProjectionExpression: &proj,
	}
	resp, err := t.dynamoClient().Query(&q)
	if err != nil {
		return nil, err
	}
	return t.buildResp(resp.Items)
}

// Delete is a method for deleting items of a Table.
//
// When using Delete, the keys parameters determine which
// items are deleted. The more keys provided, the more
// fine grained the deletions are.
func (t *Table) Delete(keys map[string]Val) error {
	avKeys, err := toAvMap(keys)
	if err != nil {
		return err
	}
	d := dynamodb.DeleteItemInput{
		TableName: &t.name,
		Key:       avKeys,
	}
	_, err = t.dynamoClient().DeleteItem(&d)
	return err
}

// Put is a method for inserting items into a Table.
//
// All items given to Put must match the structure of
// the dataType of Table.
func (t *Table) Put(item map[string]Val) error {
	avItem, err := toAvMap(item)
	if err != nil {
		return err
	}
	p := dynamodb.PutItemInput{
		TableName: &t.name,
		Item:      avItem,
	}
	_, err = t.dynamoClient().PutItem(&p)
	return err
}

func (t *Table) dynamoClient() *dynamodb.DynamoDB {
	return dynamoConf[t.conf]
}

func (t *Table) buildResp(items []map[string]*dynamodb.AttributeValue) ([]interface{}, error) {
	var values []interface{}
	for _, item := range items {
		val := reflect.New(reflect.TypeOf(t.dataType))
		for k, v := range item {
			value, err := buildValue(v)
			if err != nil {
				return nil, err
			}
			val.Elem().FieldByName(k).Set(reflect.ValueOf(value))
		}
		values = append(values, val.Elem().Interface())
	}
	return values, nil
}

var (
	dynamoConf = make(map[*aws.Config]*dynamodb.DynamoDB)
)
