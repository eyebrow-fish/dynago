package dynago

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

type Table struct {
	name     string
	dataType interface{}
}

func NewTable(name string, dataType interface{}) (*Table, error) {
	return NewTableWithConfig(name, dataType, *aws.NewConfig())
}

func NewTableWithConfig(name string, dataType interface{}, conf aws.Config) (*Table, error) {
	var err error
	sess, err = session.NewSession(&conf)
	if err != nil {
		return nil, err
	}
	dynamo = dynamodb.New(sess)
	return &Table{name, dataType}, nil
}

func (t *Table) Query(cons ...Cond) ([]interface{}, error) {
	keyCons := make(map[string]*dynamodb.Condition)
	for _, v := range cons {
		val, err := v.val.attrVal()
		if err != nil {
			return nil, err
		}
		compOp, err := v.op.compOp()
		keyCons[v.key] = &dynamodb.Condition{
			AttributeValueList: []*dynamodb.AttributeValue{val},
			ComparisonOperator: compOp,
		}
	}
	proj := projOf(t.dataType)
	q := dynamodb.QueryInput{
		TableName:            &t.name,
		KeyConditions:        keyCons,
		ProjectionExpression: &proj,
	}
	resp, err := dynamo.Query(&q)
	if err != nil {
		return nil, err
	}
	return t.buildResp(resp.Items)
}

func (t *Table) Delete(keys map[string]Val) error {
	avKeys, err := toAvMap(keys)
	if err != nil {
		return err
	}
	d := dynamodb.DeleteItemInput{
		TableName: &t.name,
		Key:       avKeys,
	}
	_, err = dynamo.DeleteItem(&d)
	return err
}

func (t *Table) Put(item map[string]Val) error {
	avItem, err := toAvMap(item)
	if err != nil {
		return err
	}
	p := dynamodb.PutItemInput{
		TableName: &t.name,
		Item:      avItem,
	}
	_, err = dynamo.PutItem(&p)
	return err
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
	sess   *session.Session
	dynamo *dynamodb.DynamoDB
)
