package test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/suite"

	"github.com/eyebrow-fish/dynago"
)

type dynamoSuite struct {
	suite.Suite
	process *os.Process
}

func (s *dynamoSuite) SetupTest()    { s.process = setupLocalDynamo() }
func (s *dynamoSuite) TearDownTest() { panicOnError(s.process.Kill()) }

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

var (
	testOptions = dynamodb.Options{
		Region:           "us-west-2",
		EndpointResolver: dynamodb.EndpointResolverFromURL("http://localhost:8000"),
		Credentials:      aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) { return aws.Credentials{}, nil }),
	}
)
