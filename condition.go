package dynago

type Condition struct {
	fieldName     string
	values        []Value
	conditionType conditionType
}

func (c Condition) rawValue() (rawValue interface{}) {
	switch c.conditionType {
	case bt, in:
		for _, value := range c.values {
			rawValue = append(rawValue.([]interface{}), value.raw)
		}
	default:
		rawValue = c.values[0].raw
	}

	return
}

func (c Condition) String() string {
	switch c.conditionType {
	case eq:
		return c.fieldName + " = :" + c.fieldName
	case neq:
		return c.fieldName + " != :" + c.fieldName
	case lt:
		return c.fieldName + " < :" + c.fieldName
	case lte:
		return c.fieldName + " <= :" + c.fieldName
	case gt:
		return c.fieldName + " > :" + c.fieldName
	case gte:
		return c.fieldName + " >= :" + c.fieldName
	case bt:
		return c.fieldName + " between :" + c.fieldName + "_lower and :" + c.fieldName + "_upper"
	default:
		return c.fieldName + " in :" + c.fieldName
	}
}

func Eq(fieldName string, value Value) Condition     { return Condition{fieldName, []Value{value}, eq} }
func Neq(fieldName string, value Value) Condition    { return Condition{fieldName, []Value{value}, neq} }
func Lt(fieldName string, value Value) Condition     { return Condition{fieldName, []Value{value}, lt} }
func Lte(fieldName string, value Value) Condition    { return Condition{fieldName, []Value{value}, lte} }
func Gt(fieldName string, value Value) Condition     { return Condition{fieldName, []Value{value}, gt} }
func Gte(fieldName string, value Value) Condition    { return Condition{fieldName, []Value{value}, gte} }
func In(fieldName string, values ...Value) Condition { return Condition{fieldName, values, in} }
func Bt(fieldName string, lower, upper Value) Condition {
	return Condition{fieldName, []Value{lower, upper}, bt}
}

type conditionType uint8

const (
	eq conditionType = iota
	neq
	lt
	lte
	gt
	gte
	in
	bt
)

type Value struct{ raw interface{} }

func S(value string) Value                 { return Value{value} }
func N(value int) Value                    { return Value{value} }
func BOOL(value bool) Value                { return Value{value} }
func B(value []byte) Value                 { return Value{value} }
func SS(value []string) Value              { return Value{value} }
func BS(value [][]byte) Value              { return Value{value} }
func NS(value []int) Value                 { return Value{value} }
func M(value map[string]interface{}) Value { return Value{value} }
func L(value []interface{}) Value          { return Value{value} }
