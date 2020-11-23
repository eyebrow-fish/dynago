package dynago

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"testing"
)

func Test_buildValue(t *testing.T) {
	type args struct {
		av *dynamodb.AttributeValue
	}
	s := "s"
	b := true
	n := 123
	ns := "123"
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{name: "string", args: args{av: &dynamodb.AttributeValue{S: &s}}, want: s},
		{name: "bool", args: args{av: &dynamodb.AttributeValue{BOOL: &b}}, want: b},
		{name: "[]byte", args: args{av: &dynamodb.AttributeValue{B: []byte{'b'}}}, want: []byte{'b'}},
		{name: "[]string", args: args{av: &dynamodb.AttributeValue{SS: []*string{&s}}}, want: []string{s}},
		{name: "[][]byte", args: args{av: &dynamodb.AttributeValue{BS: [][]byte{{'b'}}}}, want: [][]byte{{'b'}}},
		{name: "int", args: args{av: &dynamodb.AttributeValue{N: &ns}}, want: n},
		{name: "[]int", args: args{av: &dynamodb.AttributeValue{NS: []*string{&ns}}}, want: []int{n}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildValue(tt.args.av)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildValue() got = %v, want %v", got, tt.want)
			}
		})
	}
}