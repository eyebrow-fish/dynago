package dynago

type Condition struct {
	fieldName     string
	values        []Value
	conditionType conditionType
	childClause   *conditionChildClause
}

// DynamoDb has a gigantic list of reserved keywords.
// This is a super quick workaround for that. Enjoy.
func (c Condition) qualifiedFieldName() string { return c.fieldName + "_expr" }

func (c Condition) buildExpr() (string, map[string]interface{}) {
	values := make(map[string]interface{})
	values[":"+c.qualifiedFieldName()] = c.rawValue()
	expr := c.String()

	if c.childClause == nil {
		return expr, values
	}

	childExpr, childValues := c.childClause.cond.buildExpr()
	for k, v := range childValues {
		values[k] = v
	}

	return expr + c.childClause.opString() + childExpr, values
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

func (c Condition) And(condition Condition) Condition {
	c.childClause = &conditionChildClause{and, condition}

	return c
}

func (c Condition) Or(condition Condition) Condition {
	c.childClause = &conditionChildClause{or, condition}

	return c
}

func (c Condition) String() string {
	name := c.qualifiedFieldName()
	switch c.conditionType {
	case eq:
		return c.fieldName + " = :" + name
	case neq:
		return c.fieldName + " != :" + name
	case lt:
		return c.fieldName + " < :" + name
	case lte:
		return c.fieldName + " <= :" + name
	case gt:
		return c.fieldName + " > :" + name
	case gte:
		return c.fieldName + " >= :" + name
	case bt:
		return c.fieldName + " between :" + name + "_lower and :" + name + "_upper"
	default:
		return c.fieldName + " in :" + name
	}
}

func Eq(fieldName string, value Value) Condition {
	return Condition{fieldName: fieldName, values: []Value{value}, conditionType: eq}
}
func Neq(fieldName string, value Value) Condition {
	return Condition{fieldName: fieldName, values: []Value{value}, conditionType: neq}
}
func Lt(fieldName string, value Value) Condition {
	return Condition{fieldName: fieldName, values: []Value{value}, conditionType: lt}
}
func Lte(fieldName string, value Value) Condition {
	return Condition{fieldName: fieldName, values: []Value{value}, conditionType: lte}
}
func Gt(fieldName string, value Value) Condition {
	return Condition{fieldName: fieldName, values: []Value{value}, conditionType: gt}
}
func Gte(fieldName string, value Value) Condition {
	return Condition{fieldName: fieldName, values: []Value{value}, conditionType: gte}
}
func In(fieldName string, values ...Value) Condition {
	return Condition{fieldName: fieldName, values: values, conditionType: in}
}
func Bt(fieldName string, lower, upper Value) Condition {
	return Condition{fieldName: fieldName, values: []Value{lower, upper}, conditionType: bt}
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

type conditionChildClause struct {
	boolOp boolOp
	cond   Condition
}

func (c conditionChildClause) opString() string {
	switch c.boolOp {
	case and:
		return " and "
	default:
		return " or "
	}
}

type boolOp uint8

const (
	and boolOp = iota
	or
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
