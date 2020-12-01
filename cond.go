package dynago

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// A Cond is a basic condition for DynamoDb queries.
type Cond struct {
	key string
	val Val
	op  op
}

// Equals asserts the value of the item must be exactly
// equal to the given value.
func Equals(key string, val Val) Cond {
	return Cond{key, val, e}
}

// NotEquals is the exact inverse of Equals, asserting
// that the value of the item must not be equal to the
// given value.
func NotEquals(key string, val Val) Cond {
	return Cond{key, val, ne}
}

// Contains asserts the value of the item should contain
// the given value.
func Contains(key string, val Val) Cond {
	return Cond{key, val, c}
}

// NotContains is the exact inverse of Contains, asserting
// that the value of the item should not contain the given
// value.
func NotContains(key string, val Val) Cond {
	return Cond{key, val, nc}
}

// GreaterOrEquals asserts the value of the item has a
//value greater than or equal to the given value.
func GreaterOrEquals(key string, val Val) Cond {
	return Cond{key, val, ge}
}

// Greater asserts the value of the item has a value
// greater than to the given value.
func Greater(key string, val Val) Cond {
	return Cond{key, val, g}
}

// LessOrEquals asserts the value of the item has a value
// less than or equal to the given value.
func LessOrEquals(key string, val Val) Cond {
	return Cond{key, val, le}
}

// Less asserts the value of the item has a value
// less than to the given value.
func Less(key string, val Val) Cond {
	return Cond{key, val, l}
}

// In asserts the value of the item must be contained
// within the given value.
func In(key string, val Val) Cond {
	return Cond{key, val, i}
}

// Between asserts the value of the item must be between
// the first index and the second index of the given
// value.
//
// * Note that the Val given should be a slice or an array.
func Between(key string, val Val) Cond {
	return Cond{key, val, b}
}

// NotNil asserts that the value of the item is not nil.
func NotNil(key string) Cond {
	return Cond{key, nil, nn}
}

// Nil asserts that the value of the item is nil.
func Nil(key string) Cond {
	return Cond{key, nil, n}
}

// Begins asserts that the value of the item begins with
// the given value.
func Begins(key string, val Val) Cond {
	return Cond{key, val, bw}
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
