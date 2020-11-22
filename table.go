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
	var err error
	sess, err = session.NewSession(aws.NewConfig())
	if err != nil {
		return nil, err
	}
	dynamo = dynamodb.New(sess)
	return &Table{name, dataType}, nil
}

func (t *Table) Query(cons ...Cond) (interface{}, error) {
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

func (t *Table) buildResp(items []map[string]*dynamodb.AttributeValue) (interface{}, error) {
	r := reflect.New(reflect.TypeOf(t.dataType))
	for _, item := range items {
		for k, v := range item {
			if av := v.S; av != nil {
				r.FieldByName(k).SetString(*av)
			} else if av := v.BOOL; av != nil {
				r.FieldByName(k).SetBool(*av)
			} else if av := v.N; av != nil {
				n, err := strconv.Atoi(*av)
				if err != nil {
					return nil, err
				}
				r.FieldByName(k).SetInt(int64(n))
			} else if av := v.SS; av != nil {
				r.FieldByName(k).Set(reflect.ValueOf(av))
			} else if av := v.B; av != nil {
				r.FieldByName(k).SetBytes(av)
			}
		}
	}
	return r, nil
}

var (
	sess   *session.Session
	dynamo *dynamodb.DynamoDB
)
