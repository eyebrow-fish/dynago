package dynago

import (
	"reflect"
	"strings"
)

func projOf(i interface{}) string {
	value := reflect.ValueOf(i)
	var fields []string
	for i := 0; i < value.NumField(); i++ {
		fieldName := value.Type().Field(i).Name
		if f := value.Field(i); f.Kind() == reflect.Struct {
			fields = append(fields, fieldName + "." + projOf(f.Interface()))
		} else {
			fields = append(fields, fieldName)
		}
	}
	return strings.Join(fields, ",")
}
