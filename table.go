package dynago

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"strconv"
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
	q := dynamodb.QueryInput{
		TableName:     &t.name,
		KeyConditions: keyCons,
	}
	resp, err := dynamo.Query(&q)
	if err != nil {
		return nil, err
	}
	return t.buildResp(resp.Items)
}

func (t *Table) buildResp(items []map[string]*dynamodb.AttributeValue) ([]interface{}, error) {
	var values []interface{}
	for _, item := range items {
		val := reflect.New(reflect.TypeOf(t.dataType))
		for k, v := range item {
			field := val.Elem().FieldByName(k)
			if av := v.S; av != nil {
				field.SetString(*av)
			} else if av := v.BOOL; av != nil {
				field.SetBool(*av)
			} else if av := v.N; av != nil {
				n, err := strconv.Atoi(*av)
				if err != nil {
					return nil, err
				}
				field.SetInt(int64(n))
			} else if av := v.SS; av != nil {
				field.Set(reflect.ValueOf(av))
			} else if av := v.B; av != nil {
				field.SetBytes(av)
			}
		}
		values = append(values, val.Elem().Interface())
	}
	return values, nil
}

var (
	sess   *session.Session
	dynamo *dynamodb.DynamoDB
)
