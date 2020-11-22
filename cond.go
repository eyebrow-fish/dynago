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
	return Cond{key, val, e}
}

func NotEquals(key string, val Val) Cond {
	return Cond{key, val, ne}
}

func Contains(key string, val Val) Cond {
	return Cond{key, val, c}
}

func NotContains(key string, val Val) Cond {
	return Cond{key, val, nc}
}

func GreaterOrEquals(key string, val Val) Cond {
	return Cond{key, val, ge}
}

func Greater(key string, val Val) Cond {
	return Cond{key, val, g}
}

func LessOrEquals(key string, val Val) Cond {
	return Cond{key, val, le}
}

func Less(key string, val Val) Cond {
	return Cond{key, val, l}
}

func In(key string, val Val) Cond {
	return Cond{key, val, i}
}

func Between(key string, val Val) Cond {
	return Cond{key, val, b}
}

func NotNil(key string, val Val) Cond {
	return Cond{key, val, nn}
}

func Nil(key string, val Val) Cond {
	return Cond{key, val, n}
}

func Begins(key string, val Val) Cond {
	return Cond{key, val, bw}
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

type op uint8

const (
	e  op = 0
	ne op = 1
	c  op = 2
	nc op = 3
	ge op = 4
	g  op = 5
	le op = 6
	l  op = 7
	i  op = 8
	b  op = 9
	nn op = 10
	n  op = 11
	bw op = 12
)

func (o op) compOp() (*string, error) {
	var s string
	switch o {
	case e:
		s = dynamodb.ComparisonOperatorEq
	case ne:
		s = dynamodb.ComparisonOperatorNe
	case c:
		s = dynamodb.ComparisonOperatorContains
	case nc:
		s = dynamodb.ComparisonOperatorNotContains
	case ge:
		s = dynamodb.ComparisonOperatorGe
	case g:
		s = dynamodb.ComparisonOperatorGt
	case le:
		s = dynamodb.ComparisonOperatorLe
	case l:
		s = dynamodb.ComparisonOperatorLt
	case i:
		s = dynamodb.ComparisonOperatorIn
	case b:
		s = dynamodb.ComparisonOperatorBetween
	case nn:
		s = dynamodb.ComparisonOperatorNotNull
	case n:
		s = dynamodb.ComparisonOperatorNull
	case bw:
		s = dynamodb.ComparisonOperatorBeginsWith
	default:
		return nil, fmt.Errorf("could not find operator: %v", o)
	}
	return &s, nil
}
