package test

import (
	"github.com/eyebrow-fish/dynago"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewTable(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	created, _ := dynago.CreateTable("testTable", testTable{})
	fetched, err := dynago.NewTable("testTable", testTable{})

	assert.NoError(t, err)
	assert.Equal(t, created, fetched)
}

func TestNewTable_noTable(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, err := dynago.NewTable("testTable", testTable{})

	assert.Error(t, err)
}

func TestTable_QueryWithExpr(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, _ = dynago.CreateTable("testTable", testTable{})
	table, _ := dynago.NewTable("testTable", testTable{})

	item := testTable{123, "abc"}
	putValue, err := table.Put(item)
	assert.NoError(t, err)
	assert.Equal(t, putValue, item)

	testValue, err := table.QueryWithExpr("Id = :Id", map[string]interface{}{":Id": 123})
	assert.NoError(t, err)
	assert.NotEmpty(t, testValue)

	value, ok := testValue[0].(testTable)
	assert.True(t, ok)
	assert.Equal(t, testTable{123, "abc"}, value)
}

func TestTable_Query_minimalCondition(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, _ = dynago.CreateTable("testTable", testTable{})
	table, _ := dynago.NewTable("testTable", testTable{})

	item := testTable{123, "abc"}
	putValue, err := table.Put(item)
	assert.NoError(t, err)
	assert.Equal(t, putValue, item)

	testValue, err := table.Query(dynago.Eq("Id", dynago.N(123)))
	assert.NoError(t, err)
	assert.NotEmpty(t, testValue)

	value, ok := testValue[0].(testTable)
	assert.True(t, ok)
	assert.Equal(t, testTable{123, "abc"}, value)
}

func TestTable_Query_conditionsOnAllFields(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, _ = dynago.CreateTable("testTable", testTable{})
	table, _ := dynago.NewTable("testTable", testTable{})

	item := testTable{123, "abc"}
	putValue, err := table.Put(item)
	assert.NoError(t, err)
	assert.Equal(t, putValue, item)

	testValue, err := table.Query(
		dynago.Eq("Id", dynago.N(123)).
			And(dynago.Eq("FullName", dynago.S("abc"))),
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, testValue)

	value, ok := testValue[0].(testTable)
	assert.True(t, ok)
	assert.Equal(t, testTable{123, "abc"}, value)
}

func TestTable_Scan_all(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, _ = dynago.CreateTable("testTable", testTable{})
	table, _ := dynago.NewTable("testTable", testTable{})

	item1 := testTable{123, "abc"}
	putValue1, err := table.Put(item1)
	assert.NoError(t, err)
	assert.Equal(t, item1, putValue1)

	item2 := testTable{456, "def"}
	putValue2, err := table.Put(item2)
	assert.NoError(t, err)
	assert.Equal(t, item2, putValue2)

	scan, err := table.ScanAll()
	assert.NoError(t, err)

	value1, ok1 := scan[0].(testTable)
	assert.True(t, ok1)
	assert.Equal(t, testTable{123, "abc"}, value1)

	value2, ok2 := scan[1].(testTable)
	assert.True(t, ok2)
	assert.Equal(t, testTable{456, "def"}, value2)
}

func TestTable_Put_conditionFails(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, _ = dynago.CreateTable("testTable", testTable{})
	table, _ := dynago.NewTable("testTable", testTable{})

	item := testTable{68, "abc"}
	_, err := table.PutWithCondition(dynago.Gte("Id", dynago.N(69)), item)
	assert.Error(t, err)
}

func TestTable_Put_conditionPasses(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, _ = dynago.CreateTable("testTable", testTable{})
	table, _ := dynago.NewTable("testTable", testTable{})

	item := testTable{69, "abc"}
	_, err := table.PutWithCondition(dynago.Gte("Id", dynago.N(69)), item)
	assert.Error(t, err)
}

type testTable struct {
	Id       int
	FullName string
}
