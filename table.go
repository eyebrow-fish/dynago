package dynago

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Table struct {
	name string
}

func NewTable(name string) (*Table, error) {
	var err error
	sess, err = session.NewSession(aws.NewConfig())
	if err != nil {
		return nil, err
	}
	dynamo = dynamodb.New(sess)
	return &Table{name}, nil
}

func (t *Table) Query(conds ...Cond) (interface{}, error) {
	keyConds := make(map[string]*dynamodb.Condition)
	for _, v := range conds {
		val, err := v.val.attrVal()
		if err != nil {
			return nil, err
		}
		compOp, err := v.op.compOp()
		keyConds[v.key] = &dynamodb.Condition{
			AttributeValueList: []*dynamodb.AttributeValue{val},
			ComparisonOperator: compOp,
		}
	}
	q := dynamodb.QueryInput{
		TableName:     &t.name,
		KeyConditions: keyConds,
	}
	return dynamo.Query(&q)
}

var (
	sess   *session.Session
	dynamo *dynamodb.DynamoDB
)
