package test

import (
	"testing"

	"github.com/eyebrow-fish/dynago"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type NewTableSuite struct{ dynamoSuite }

func (s *NewTableSuite) TestHappyPath() {
	created, _ := dynago.CreateTable("testTable", testTable{})
	fetched, err := dynago.NewTable("testTable", testTable{})

	assert.NoError(s.T(), err)
	assert.Equal(s.T(), created, fetched)
}

func (s *NewTableSuite) TestNoTable() {
	_, err := dynago.NewTable("testTable", testTable{})

	assert.Error(s.T(), err)
}

func TestNewTable(t *testing.T) { suite.Run(t, new(NewTableSuite)) }

type QuerySuite struct{ dynamoSuite }

func (s *QuerySuite) TestQueryWithExpr() {
	table, _ := dynago.CreateTable("testTable", testTable{})

	item := testTable{123, "abc"}
	putValue, err := table.Put(item)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), putValue, item)

	testValue, err := table.QueryWithExpr("Id = :Id", map[string]interface{}{":Id": 123})
	assert.NoError(s.T(), err)
	assert.NotEmpty(s.T(), testValue)

	value, ok := testValue[0].(testTable)
	assert.True(s.T(), ok)
	assert.Equal(s.T(), testTable{123, "abc"}, value)
}

func (s *QuerySuite) TestMinimalCondition() {
	table, _ := dynago.CreateTable("testTable", testTable{})

	item := testTable{123, "abc"}
	putValue, err := table.Put(item)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), putValue, item)

	testValue, err := table.Query(dynago.Eq("Id", dynago.N(123)))
	assert.NoError(s.T(), err)
	assert.NotEmpty(s.T(), testValue)

	value, ok := testValue[0].(testTable)
	assert.True(s.T(), ok)
	assert.Equal(s.T(), testTable{123, "abc"}, value)
}

func (s *QuerySuite) TestConditionsOnAllFields() {
	table, _ := dynago.CreateTable("testTable", testTable{})

	item := testTable{123, "abc"}
	putValue, err := table.Put(item)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), putValue, item)

	testValue, err := table.Query(
		dynago.Eq("Id", dynago.N(123)).
			And(dynago.Eq("FullName", dynago.S("abc"))),
	)
	assert.NoError(s.T(), err)
	assert.NotEmpty(s.T(), testValue)

	value, ok := testValue[0].(testTable)
	assert.True(s.T(), ok)
	assert.Equal(s.T(), testTable{123, "abc"}, value)
}

func TestQuery(t *testing.T) { suite.Run(t, new(QuerySuite)) }

type ScanSuite struct{ dynamoSuite }

func (s *ScanSuite) TestAll() {
	table, _ := dynago.CreateTable("testTable", testTable{})

	item1 := testTable{123, "abc"}
	putValue1, err := table.Put(item1)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), item1, putValue1)

	item2 := testTable{456, "def"}
	putValue2, err := table.Put(item2)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), item2, putValue2)

	scan, err := table.ScanAll()
	assert.NoError(s.T(), err)

	value1, ok1 := scan[0].(testTable)
	assert.True(s.T(), ok1)
	assert.Equal(s.T(), testTable{123, "abc"}, value1)

	value2, ok2 := scan[1].(testTable)
	assert.True(s.T(), ok2)
	assert.Equal(s.T(), testTable{456, "def"}, value2)
}

func TestScan(t *testing.T) { suite.Run(t, new(ScanSuite)) }

type PutSuite struct{ dynamoSuite }

func (s *PutSuite) ConditionFails() {
	table, _ := dynago.CreateTable("testTable", testTable{})

	item := testTable{68, "abc"}
	_, err := table.PutWithCondition(dynago.Gte("Id", dynago.N(69)), item)
	assert.Error(s.T(), err)
}

func (s *PutSuite) ConditionPasses() {
	table, _ := dynago.CreateTable("testTable", testTable{})

	item := testTable{69, "abc"}
	_, err := table.PutWithCondition(dynago.Gte("Id", dynago.N(69)), item)
	assert.Error(s.T(), err)
}

func TestPut(t *testing.T) { suite.Run(t, new(PutSuite)) }

type testTable struct {
	Id       int
	FullName string
}
