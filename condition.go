package dynago

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strconv"
)

type Condition struct {
	fieldName     string
	values        []Value
	conditionType conditionType
}

func Equals(fieldName string, value Value) Condition { return Condition{fieldName, []Value{value}, eq} }
func NotEquals(fieldName string, value Value) Condition {
	return Condition{fieldName, []Value{value}, neq}
}
func LessThan(fieldName string, value Value) Condition {
	return Condition{fieldName, []Value{value}, lt}
}
func LessThanOrEquals(fieldName string, value Value) Condition {
	return Condition{fieldName, []Value{value}, lte}
}
func GreaterThan(fieldName string, value Value) Condition {
	return Condition{fieldName, []Value{value}, gt}
}
func GreaterThanOrEquals(fieldName string, value Value) Condition {
	return Condition{fieldName, []Value{value}, gte}
}
func Between(fieldName string, lower, upper Value) Condition {
	return Condition{fieldName, []Value{lower, upper}, bt}
}
func In(fieldName string, values ...Value) Condition { return Condition{fieldName, values, in} }

type conditionType uint8

const (
	eq conditionType = iota
	neq
	lt
	lte
	gt
	gte
	bt
	in
)

type Value struct {
	attrValue types.AttributeValue
}

func String(value string) Value    { return Value{&types.AttributeValueMemberS{Value: value}} }
func Number(value int) Value       { return Value{&types.AttributeValueMemberN{Value: strconv.Itoa(value)}} }
func Bool(value bool) Value        { return Value{&types.AttributeValueMemberBOOL{Value: value}} }
func Bytes(value []byte) Value     { return Value{&types.AttributeValueMemberB{Value: value}} }
func Strings(value []string) Value { return Value{&types.AttributeValueMemberSS{Value: value}} }
func Bytes2d(value [][]byte) Value { return Value{&types.AttributeValueMemberBS{Value: value}} }
func Numbers(value []int) Value {
	var numbers []string
	for _, v := range value {
		numbers = append(numbers, strconv.Itoa(v))
	}

	return Value{&types.AttributeValueMemberNS{Value: numbers}}
}
func Map(value map[string]interface{}) Value {
	mapValue := make(map[string]types.AttributeValue)
	for k, v := range value {
		mapValue[k] = toAttributeValue(v)
	}

	return Value{&types.AttributeValueMemberM{Value: mapValue}}
}
func List(value []interface{}) Value {
	var attributes []types.AttributeValue
	for _, v := range value {
		attributes = append(attributes, toAttributeValue(v))
	}

	return Value{&types.AttributeValueMemberL{Value: attributes}}
}
