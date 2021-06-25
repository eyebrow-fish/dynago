package dynago_test

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/eyebrow-fish/dynago"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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

var (
	testOptions = dynamodb.Options{
		Region:           "us-west-2",
		EndpointResolver: dynamodb.EndpointResolverFromURL("http://localhost:8000"),
		Credentials:      aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) { return aws.Credentials{}, nil }),
	}
)

type testTable struct {
	Id   int
	Name string
}

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

	dynago.UpdateOptions(testOptions)

	return command.Process
}

func panicOnError(err error) {
	if err == nil {
		return
	}

	panic(err)
}
