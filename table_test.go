package dynago_test

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/eyebrow-fish/dynago"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCreateTable(t *testing.T) {
	type TestTable struct {
		Id   int
		Name string
	}

	process := setupLocalDynamo()
	defer func() { panicOnError(process.Kill()) }()

	table, err := dynago.CreateTableWithOptions("TestTable", TestTable{}, testOptions)
	if err != nil {
		t.Fatal("error creating table:", err)
	}
	if table == nil {
		t.Fatal("table was nil")
	}
	if table.Name != "TestTable" {
		t.Fatal("table was not called TestTable")
	}
}

var (
	testOptions = dynamodb.Options{
		Region:           "us-west-2",
		EndpointResolver: dynamodb.EndpointResolverFromURL("http://localhost:8000"),
		Credentials:      aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) { return aws.Credentials{}, nil }),
	}
)

func setupLocalDynamo() *os.Process {
	homeDir, err := os.UserHomeDir()
	panicOnError(err)

	libDir := filepath.Join(homeDir, "dev", "dynamo-local-lib")

	command := exec.Command(
		"java",
		"-Djava.library.path="+filepath.Join(libDir, "DynamoDBLocal_lib"),
		"-jar",
		filepath.Join(libDir, "DynamoDBLocal.jar"),
		"-inMemory",
	)

	panicOnError(command.Start())
	panicOnError(exec.Command("aws", "dynamodb", "list-tables", "--endpoint-url", "http://localhost:8000").Run())

	return command.Process
}

func panicOnError(err error) {
	if err == nil {
		return
	}

	panic(err)
}
