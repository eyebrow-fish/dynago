package test

import (
	"testing"

	"github.com/eyebrow-fish/dynago"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CreateTableSuite struct{ DynamoSuite }

func (s *CreateTableSuite) TestHappyPath() {
	table, err := dynago.CreateTable("testTable", testTable{})

	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), table)
	assert.Equal(s.T(), &dynago.Table{Name: "testTable", Schema: testTable{}, Projection: "Id,FullName"}, table)
}

func (s *CreateTableSuite) TestDuplicate() {
	_, _ = dynago.CreateTable("testTable", testTable{})
	_, err := dynago.CreateTable("testTable", testTable{})
	assert.Error(s.T(), err)
}

func (s *CreateTableSuite) TestNoHash() {
	_, err := dynago.CreateTable("testTable", struct{}{})
	assert.Error(s.T(), err)
}

func TestCreateTable(t *testing.T) { suite.Run(t, new(CreateTableSuite)) }

func TestListTables(t *testing.T) {
	process := SetupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	_, _ = dynago.CreateTable("testTable1", testTable{})
	_, _ = dynago.CreateTable("testTable2", testTable{})

	tableNames, err := dynago.ListTables()
	assert.NoError(t, err)

	assert.Equal(t, []string{"testTable1", "testTable2"}, tableNames)
}
