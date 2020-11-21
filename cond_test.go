package dynago

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"testing"
)

func TestVal_attrVal_string(t *testing.T) {
	inner := "string"
	v := NewVal(inner)
	ex := dynamodb.AttributeValue{S: &inner}

	av, err := v.attrVal()

	if err != nil {
		t.Fatalf("expected error to be nil")
	}
	if !reflect.DeepEqual(*av, ex) {
		t.Fatalf("expected %v to equal %v", av, ex)
	}
}
