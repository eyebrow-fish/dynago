package dynago

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

type Cond struct {
	key string
	val Val
	op  op
}

func Equals(key string, val Val) Cond {
	return Cond{key, val, eq}
}

type Val struct {
	val interface{}
}

func NewVal(v interface{}) Val {
	return Val{v}
}

func (v Val) attrVal() (*dynamodb.AttributeValue, error) {
	value := reflect.TypeOf(v.val)
	switch value.Kind() {
	case reflect.String:
		s := value.String()
		return &dynamodb.AttributeValue{S: &s}, nil
	}
	return nil, fmt.Errorf("invalid AttributeValue: %v", v)
}

type op uint8

const (
	eq op = 0
)

func (o op) compOp() (*string, error) {
	var s string
	switch o {
	case eq:
		s = dynamodb.ComparisonOperatorEq
	default:
		return nil, fmt.Errorf("could not find operator: %v", o)
	}
	return &s, nil
}
