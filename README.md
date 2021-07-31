# dynago

`dynago` is an extensive wrapper around the [AWS Go Sdk (V2)](https://github.com/aws/aws-sdk-go-v2) — which I find to
not be particularly developer-friendly.

# example

Queries, scans, puts, and deletions are fundamental to `dynago`. These are all available once a `Table` is initialized
with a schema given by an interface. Here is a simple example:

```go
package main

import (
	"fmt"
	"github.com/eyebrow-fish/dynago"
)

type Person struct {
	Country           string
	Age               uint8
	FirstName         string
	LastName          string
	PresidentialTerms uint8
}

// Let's create a slice of eligible presidents in the USA.
func main() {
	person, err := dynago.NewTable("Person", Person{})
	if err != nil {
		panic(err) // TODO: Better error handling
	}

	eligiblePresidents, err := person.Query(
		dynago.Eq("Country", dynago.S("United States of America")).
			And(dynago.Gte("Age", dynago.N(35))).
			And(dynago.Lt("PresidentialTerms", dynago.N(2))),
	)
	if err != nil {
		panic(err) // Oh noes
	}

	fmt.Printf("Eligible presidents: %v\n", eligiblePresidents)

	// TODO: Select new President
	// TODO: Update "PresidentialTerms" of new President
}
```

All fetching-oriented methods will be paginated, which is important to bare-in-mind for scanning.
In general, scans should be used sparingly, unless your tables are incredibly small.

# development

The local dynamodb JAR is a must. Without this you cannot run the tests.

**Setup**:

- Download the
  JAR [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html)
- Unzip to `~/dev/dynamo-local-lib` *(eg. unzip dynamodb_local_latest.zip -d ~/dev/dynamo-local-lib)*
- You're done! Tests **SHOULD** just work.
