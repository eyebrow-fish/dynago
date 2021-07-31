package dynago

import "reflect"

// buildProjection builds the projections used in DynamoDb
// queries based off the given interface{}.
func buildProjection(schema interface{}) (proj string) {
	schemaType := reflect.TypeOf(schema)

	schemaLength := schemaType.NumField()
	for i := 0; i < schemaLength; i++ {
		field := schemaType.Field(i)

		proj += field.Name
		if i < schemaLength-1 {
			proj += ","
		}
	}

	return
}
