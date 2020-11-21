package dynago

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"strconv"
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
	value := reflect.ValueOf(v.val)
	switch value.Kind() {
	case reflect.String:
		s := value.String()
		return &dynamodb.AttributeValue{S: &s}, nil
	case reflect.Bool:
		b := value.Bool()
		return &dynamodb.AttributeValue{BOOL: &b}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ns := strconv.Itoa(int(value.Int()))
		return &dynamodb.AttributeValue{N: &ns}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		ns := strconv.Itoa(int(value.Uint()))
		return &dynamodb.AttributeValue{N: &ns}, nil
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
