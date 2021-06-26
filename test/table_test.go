package test

import (
	"github.com/eyebrow-fish/dynago"
	"reflect"
	"testing"
)

func TestNewTable(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	created, _ := dynago.CreateTable("testTable", testTable{})
	fetched, err := dynago.NewTable("testTable", testTable{})
	if err != nil {
		t.Fatal("error creating table:", err)
	}
	if !reflect.DeepEqual(created, fetched) {
		t.Fatal("expected", *created, "but got", *fetched)
	}
}

func TestNewTable_noTable(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, err := dynago.NewTable("testTable", testTable{})
	if err == nil {
		t.Fatal("expected an error to occur")
	}
}

func TestTable_Query(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, _ = dynago.CreateTable("testTable", testTable{})
	table, _ := dynago.NewTable("testTable", testTable{})

	_, err := table.Put(testTable{123, "abc"})
	if err != nil {
		t.Fatal("unexpected error when inserting:", err)
	}

	testValue, err := table.QueryWithExpr("Id = :Id", map[string]interface{}{":Id": 123})
	if err != nil {
		t.Fatal("unexpected error when querying:", err)
	}
	if len(testValue) == 0 {
		t.Fatal("expected at least one response item")
	}

	value, ok := testValue[0].(testTable)
	if !ok {
		t.Fatal("response was not testTable")
	}
	if value.Id != 123 {
		t.Fatal("expected 123 but got", value.Id)
	}
	if value.Name != "abc" {
		t.Fatal("expected abc but got", value.Name)
	}
}

type testTable struct {
	Id   int
	Name string
}