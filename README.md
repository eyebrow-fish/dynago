# dynago

`dynago` is a super easy to use [DynamoDb](https://aws.amazon.com/dynamodb) library for [golang](https://golang.org).
The philosophy behind `dynago` is to keep it simple. This removes the need for complex data structures to represent
queries and mutations.

# example

A simple `Query` example.

```go
package main

import (
    "fmt"
    "github.com/eyebrow-fish/dynago"
    "log"
)

func main() {
    table, err := dynago.NewTable("my-table")
    if err != nil {
        log.Fatalf("could not init table client: %v", err)
    }
    resp, err := table.Query(
        dynago.Equals("part", dynago.NewVal("value")),
        dynago.Equals("sort", dynago.NewVal(1234567)),
    )
    if err != nil {
        log.Fatalf("error in query: %v", err)
    }
    fmt.Println(resp)
}
```