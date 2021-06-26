package test

import (
	"os"
	"os/exec"
	"path/filepath"

	"github.com/eyebrow-fish/dynago"
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

	dynago.UpdateOptions(testOptions)

	return command.Process
}

func panicOnError(err error) {
	if err == nil {
		return
	}

	panic(err)
}
