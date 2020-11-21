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
		{"string", NewVal("s"), dynamodb.AttributeValue{S: &s}, false},
		{"bool", NewVal(true), dynamodb.AttributeValue{BOOL: &b}, false},
		{"int", NewVal(123), dynamodb.AttributeValue{N: &n}, false},
		{"uint", NewVal(uint(123)), dynamodb.AttributeValue{N: &n}, false},
		{"[]byte", NewVal([]byte{'f'}), dynamodb.AttributeValue{B: []byte{'f'}}, false},
		{"[]string", NewVal([]string{"s"}), dynamodb.AttributeValue{SS: []*string{&s}}, false},
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
