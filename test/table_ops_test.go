package test

import (
	"github.com/eyebrow-fish/dynago"
	"testing"
)

func TestCreateTable(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	table, err := dynago.CreateTable("testTable", testTable{})
	if err != nil {
		t.Fatal("error creating table:", err)
	}
	if table == nil {
		t.Fatal("table was nil")
	}
	if table.Name != "testTable" {
		t.Fatal("table was not called testTable")
	}
	if table.Schema != (testTable{}) {
		t.Fatal("table was not", testTable{})
	}
}

func TestCreateTable_duplicate(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, _ = dynago.CreateTable("testTable", testTable{})
	_, err := dynago.CreateTable("testTable", testTable{})
	if err == nil {
		t.Fatal("expected an error to occur")
	}
}

func TestCreateTable_noHash(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, err := dynago.CreateTable("testTable", struct{}{})
	if err == nil {
		t.Fatal("expected an error to occur")
	}
}
