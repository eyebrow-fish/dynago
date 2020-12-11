# dynago

`dynago` is a super easy to use [DynamoDb](https://aws.amazon.com/dynamodb) library for [golang](https://golang.org).
The philosophy behind `dynago` is to keep it simple. This removes the need for complex data structures to represent
queries and mutations.

# features

`dynago` is currently in a usable state, but it is decently unstable.

There are a small set of features currently developed:
 - Projections (uses table `dataType`)
 - Queries
 - Scans
 - Deletes
 - Puts

# example

A simple `Query` example.

```go
package main

import (
    "fmt"
    "github.com/eyebrow-fish/dynago"
    "log"
)

type GenericItem struct {
    Part string
    Sort int
}

func main() {
    table, err := dynago.NewTable("my-table", GenericItem{})
    if err != nil {
        log.Fatalf("could not init table client: %v", err)
    }
    resp, err := table.Query(
        dynago.Equals("Part", dynago.NewVal("value")),
        dynago.Equals("Sort", dynago.NewVal(1234567)),
    )
    if err != nil {
        log.Fatalf("error in query: %v", err)
    }
    fmt.Println(resp[0].(GenericItem).Part)
}
```
