package dynago

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
	"testing"
)

func Test_fromAttribute(t *testing.T) {
	type args struct {
		attribute types.AttributeValue
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{"string", args{&types.AttributeValueMemberS{Value: "foo"}}, "foo", false},
		{"number", args{&types.AttributeValueMemberN{Value: "123"}}, 123, false},
		{"bytes", args{&types.AttributeValueMemberB{Value: []byte{1}}}, []byte{1}, false},
		{"strings", args{&types.AttributeValueMemberSS{Value: []string{"foo", "bar"}}}, []string{"foo", "bar"}, false},
		{"numbers", args{&types.AttributeValueMemberNS{Value: []string{"123", "456"}}}, []int{123, 456}, false},
		{"numbers", args{&types.AttributeValueMemberNS{Value: []string{"123", "456"}}}, []int{123, 456}, false},
		{"2d bytes", args{&types.AttributeValueMemberBS{Value: [][]byte{{1}, {2}}}}, [][]byte{{1}, {2}}, false},
		{"map", args{&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"foo": &types.AttributeValueMemberS{Value: "bar"}}}}, map[string]interface{}{"foo": "bar"}, false},
		{"list", args{&types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberS{Value: "bar"}}}}, []interface{}{"bar"}, false},
		{"bool", args{&types.AttributeValueMemberBOOL{}}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fromAttribute(tt.args.attribute)
			if (err != nil) != tt.wantErr {
				t.Errorf("fromAttribute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("fromAttribute() got = %v, want %v", got, tt.want)
			}
		})
	}
}
