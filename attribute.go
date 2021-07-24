package dynago

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
	"strconv"
)

func fromMap(values map[string]interface{}) map[string]types.AttributeValue {
	if values == nil {
		return nil
	}

	attributeValues := make(map[string]types.AttributeValue)
	for k, v := range values {
		attributeValues[k] = toAttributeValue(v)
	}

	return attributeValues
}

func constructItems(items []map[string]types.AttributeValue, to interface{}) ([]interface{}, error) {
	var outputItems []interface{}
	for _, item := range items {
		i, err := constructItem(item, to)
		if err != nil {
			return nil, err
		}

		outputItems = append(outputItems, i)
	}

	return outputItems, nil
}

func constructItem(item map[string]types.AttributeValue, to interface{}) (interface{}, error) {
	itemType := reflect.New(reflect.TypeOf(to))
	itemValue := itemType.Elem()

	for k, v := range item {
		attribute, err := fromAttribute(v)
		if err != nil {
			return nil, err
		}

		itemValue.
			FieldByName(k).
			Set(reflect.ValueOf(attribute))
	}

	return itemValue.Interface(), nil
}

func buildItem(item interface{}) map[string]types.AttributeValue {
	itemValue := reflect.ValueOf(item)
	itemType := reflect.TypeOf(item)
	attributeValue := make(map[string]types.AttributeValue)

	for i := 0; i < itemValue.NumField(); i++ {
		value := toAttributeValue(itemValue.Field(i).Interface())

		attributeValue[itemType.Field(i).Name] = value
	}

	return attributeValue
}

func fromAttribute(attribute types.AttributeValue) (interface{}, error) {
	switch attribute.(type) {
	case *types.AttributeValueMemberS:
		return attribute.(*types.AttributeValueMemberS).Value, nil
	case *types.AttributeValueMemberN:
		return strconv.Atoi(attribute.(*types.AttributeValueMemberN).Value)
	case *types.AttributeValueMemberB:
		return attribute.(*types.AttributeValueMemberB).Value, nil
	case *types.AttributeValueMemberSS:
		return attribute.(*types.AttributeValueMemberSS).Value, nil
	case *types.AttributeValueMemberNS:
		var numbers []int

		for _, v := range attribute.(*types.AttributeValueMemberNS).Value {
			number, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}

			numbers = append(numbers, number)
		}

		return numbers, nil
	case *types.AttributeValueMemberBS:
		return attribute.(*types.AttributeValueMemberBS).Value, nil
	case *types.AttributeValueMemberM:
		dict := attribute.(*types.AttributeValueMemberM).Value

		values := make(map[string]interface{})
		for k, v := range dict {
			value, err := fromAttribute(v)
			if err != nil {
				return nil, err
			}

			values[k] = value
		}

		return values, nil
	case *types.AttributeValueMemberL:
		list := attribute.(*types.AttributeValueMemberL).Value

		var values []interface{}
		for _, i := range list {
			value, err := fromAttribute(i)
			if err != nil {
				return nil, err
			}

			values = append(values, value)
		}

		return values, nil
	case *types.AttributeValueMemberNULL:
		return attribute.(*types.AttributeValueMemberNULL).Value, nil
	default:
		return attribute.(*types.AttributeValueMemberBOOL).Value, nil
	}
}

func toAttributeValue(value interface{}) types.AttributeValue {
	switch value.(type) {
	case string:
		return &types.AttributeValueMemberS{Value: value.(string)}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128:
		return &types.AttributeValueMemberN{Value: strconv.Itoa(value.(int))}
	case []byte:
		return &types.AttributeValueMemberB{Value: value.([]byte)}
	case []string:
		return &types.AttributeValueMemberSS{Value: value.([]string)}
	case []int, []int8, []int16, []int32, []int64, []uint, []uint16, []uint32, []uint64, []float32, []float64, []complex64, []complex128:
		var numbers []string

		for _, i := range value.([]int) {
			numbers = append(numbers, strconv.Itoa(i))
		}

		return &types.AttributeValueMemberSS{Value: numbers}
	case [][]byte:
		return &types.AttributeValueMemberBS{Value: value.([][]byte)}
	case map[string]interface{}:
		mapValues := make(map[string]types.AttributeValue)

		for k, v := range value.(map[string]interface{}) {
			mapValues[k] = toAttributeValue(v)
		}

		return &types.AttributeValueMemberM{Value: mapValues}
	case []interface{}:
		var values []types.AttributeValue

		for _, v := range value.([]interface{}) {
			values = append(values, toAttributeValue(v))
		}

		return &types.AttributeValueMemberL{Value: values}
	default:
		return &types.AttributeValueMemberBOOL{Value: value.(bool)}
	}
}

func toAttributeType(value interface{}) (types.ScalarAttributeType, error) {
	switch value.(type) {
	case string:
		return "S", nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128:
		return "N", nil
	case []byte:
		return "B", nil
	case []string:
		return "SS", nil
	case []int, []int8, []int16, []int32, []int64, []uint, []uint16, []uint32, []uint64, []float32, []float64, []complex64, []complex128:
		return "NS", nil
	case [][]byte:
		return "BS", nil
	case map[string]interface{}, struct{}:
		return "M", nil
	case []interface{}:
		return "L", nil
	case bool:
		return "BOOL", nil
	default:
		return "", fmt.Errorf("unsupported type: %v", reflect.TypeOf(value))
	}
}
