package dynago

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"strconv"
)

// A Val is a wrapper for dynamodb.AttributeValue.
//
// The reason for it's existence is the tidy up the
// construction of said dynamodb.AttributeValue(s).
type Val struct {
	val interface{}
}

// NewVal constructs a Val of the given type.
//
// For example, passing a string in will create an
// "S" dynamodb.AttributeValue.
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
	case reflect.Int:
		ns := strconv.Itoa(int(value.Int()))
		return &dynamodb.AttributeValue{N: &ns}, nil
	case reflect.Slice:
		switch value.Index(0).Kind() {
		case reflect.String:
			ss := value.Interface().([]string)
			var strings []*string
			for _, v := range ss {
				strings = append(strings, &v)
			}
			return &dynamodb.AttributeValue{SS: strings}, nil
		case reflect.Int:
			ns := value.Interface().([]int)
			var nums []*string
			for _, v := range ns {
				n := strconv.Itoa(v)
				nums = append(nums, &n)
			}
			return &dynamodb.AttributeValue{NS: nums}, nil
		case reflect.Uint8:
			bytes := value.Bytes()
			return &dynamodb.AttributeValue{B: bytes}, nil
		default:
			is := value.Interface().([]Val)
			var values []*dynamodb.AttributeValue
			for _, v := range is {
				val, err := v.attrVal()
				if err != nil {
					return nil, err
				}
				values = append(values, val)
			}
			return &dynamodb.AttributeValue{L: values}, nil
		}
	case reflect.Map:
		m := make(map[string]*dynamodb.AttributeValue)
		for _, k := range value.MapKeys() {
			val, err := NewVal(value.MapIndex(k).Interface()).attrVal()
			if err != nil {
				return nil, err
			}
			m[k.String()] = val
		}
		return &dynamodb.AttributeValue{M: m}, nil
	}
	return nil, fmt.Errorf("invalid AttributeValue: %v", v)
}
