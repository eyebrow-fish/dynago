package dynago

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"testing"
)

func TestVal_attrVal(t *testing.T) {
	s := "s"
	b := true
	n := "123"
	tests := []struct {
		name    string
		val     Val
		want    dynamodb.AttributeValue
		wantErr bool
	}{
		{name: "string", val: NewVal("s"), want: dynamodb.AttributeValue{S: &s}},
		{name: "bool", val: NewVal(true), want: dynamodb.AttributeValue{BOOL: &b}},
		{name: "int", val: NewVal(123), want: dynamodb.AttributeValue{N: &n}},
		{name: "[]byte", val: NewVal([]byte{'f'}), want: dynamodb.AttributeValue{B: []byte{'f'}}},
		{name: "[]string", val: NewVal([]string{"s"}), want: dynamodb.AttributeValue{SS: []*string{&s}}},
		{name: "[]int", val: NewVal([]int{123}), want: dynamodb.AttributeValue{NS: []*string{&n}}},
		{name: "[]val", val: NewVal([]Val{NewVal("s")}), want: dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{{S: &s}}}},
		{name: "map", val: NewVal(map[string]string{"k": "s"}), want: dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{"k": {S: &s}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.val.attrVal()
			if tt.wantErr == (err == nil) {
				t.Fatalf("unexpected error state")
			}
			if err == nil && !reflect.DeepEqual(*got, tt.want) {
				t.Fatalf("expected %v to equal %v", got, tt.want)
			}
		})
	}
}
