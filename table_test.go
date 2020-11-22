package dynago

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"strconv"
	"testing"
)

func TestTable_buildResp(t *testing.T) {
	type TestItem struct {
		Part string
		Sort int
	}
	mockTable := Table{dataType: TestItem{}}
	part0 := "part0"
	ns0 := strconv.Itoa(1234567)
	part1 := "part0"
	ns1 := strconv.Itoa(1234567)
	expected0 := TestItem{part0, 1234567}
	expected1 := TestItem{part0, 1234567}

	resp, err := mockTable.buildResp([]map[string]*dynamodb.AttributeValue{
		{"Part": &dynamodb.AttributeValue{S: &part0}, "Sort": &dynamodb.AttributeValue{N: &ns0}},
		{"Part": &dynamodb.AttributeValue{S: &part1}, "Sort": &dynamodb.AttributeValue{N: &ns1}},
	})

	if err != nil {
		t.Fatal(err)
	}
	if got := resp[0].(TestItem); !reflect.DeepEqual(got, expected0) {
		t.Fatalf("expected %v to equal %v", got, expected0)
	}
	if got := resp[1].(TestItem); !reflect.DeepEqual(got, expected1) {
		t.Fatalf("expected %v to equal %v", got, expected1)
	}
}
