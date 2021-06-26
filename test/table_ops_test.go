package test

import (
	"github.com/eyebrow-fish/dynago"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateTable(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	table, err := dynago.CreateTable("testTable", testTable{})
	assert.NoError(t, err)
	assert.NotNil(t, table)
	assert.Equal(t, &dynago.Table{Name: "testTable", Schema: testTable{}}, table)
}

func TestCreateTable_duplicate(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, _ = dynago.CreateTable("testTable", testTable{})
	_, err := dynago.CreateTable("testTable", testTable{})
	assert.Error(t, err)
}

func TestCreateTable_noHash(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, err := dynago.CreateTable("testTable", struct{}{})
	assert.Error(t, err)
}

func TestListTables(t *testing.T) {
	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, _ = dynago.CreateTable("testTable1", testTable{})
	_, _ = dynago.CreateTable("testTable2", testTable{})

	tableNames, err := dynago.ListTables()
	assert.NoError(t, err)

	assert.Equal(t, []string{"testTable1", "testTable2"}, tableNames)
}
