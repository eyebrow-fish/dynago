package dynago_test

import (
	"github.com/eyebrow-fish/dynago"
	"os"
	"os/exec"
	"testing"
)

func TestCreateTable(t *testing.T) {
	type TestTable struct{}

	process := setupLocalDynamo()
	defer panicOnError(process.Kill())

	table, err := dynago.CreateTable("TestTable", TestTable{})
	if err != nil {
		t.Fatal("error creating table:", err)
	}
	if table == nil {
		t.Fatal("table was nil")
	}
	if table.Name == "TestTable" {
		t.Fatal("table was not called TestTable")
	}
}

func setupLocalDynamo() *os.Process {
	command := exec.Command(
		"java",
		"-Djava.library.path=~/dev/dynamo-local-lib/DynamoDBLocal_lib",
		"-jar",
		"~/dev/dynamo-local-lib/DynamoDBLocal.jar",
		"-sharedDb",
	)

	panicOnError(command.Start())

	return command.Process
}

func panicOnError(err error) {
	if err == nil {
		return
	}

	panic(err)
}
