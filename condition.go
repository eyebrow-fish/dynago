package dynago

type Condition struct {
	fieldName     string
	values        []Value
	conditionType conditionType
	childClause   *conditionChildClause
	options       *conditionOptions
}

type conditionOptions struct {
	limit *int32
}

// DynamoDb has a gigantic list of reserved keywords.
// This is a super quick workaround for that. Enjoy.
func (c Condition) qualifiedFieldName() string { return c.fieldName + "_expr" }

func (c Condition) buildExpr() (*string, map[string]interface{}) {
	if c.conditionType == all {
		return nil, nil
	}

	values := make(map[string]interface{})
	values[":"+c.qualifiedFieldName()] = c.rawValue()
	currExpr := c.String()

	if c.childClause == nil {
		return &currExpr, values
	}

	childExpr, childValues := c.childClause.cond.buildExpr()
	for k, v := range childValues {
		values[k] = v
	}

	expr := currExpr + c.childClause.opString() + *childExpr
	return &expr, values
}

func (c Condition) rawValue() (rawValue interface{}) {
	switch c.conditionType {
	case bt:
		rawValue = []interface{}{}
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
	c.childClause.cond.options = c.options

	return c
}

func (c Condition) Or(condition Condition) Condition {
	c.childClause = &conditionChildClause{or, condition}
	c.childClause.cond.options = c.options

	return c
}

func (c Condition) WithLimit(limit int32) Condition {
	c.options.limit = &limit

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
		return "" // Special cases
	}
}

func All() Condition                              { return Condition{conditionType: all, options: new(conditionOptions)} }
func Eq(fieldName string, value Value) Condition  { return newCond(fieldName, []Value{value}, eq) }
func Neq(fieldName string, value Value) Condition { return newCond(fieldName, []Value{value}, neq) }
func Lt(fieldName string, value Value) Condition  { return newCond(fieldName, []Value{value}, lt) }
func Lte(fieldName string, value Value) Condition { return newCond(fieldName, []Value{value}, lte) }
func Gt(fieldName string, value Value) Condition  { return newCond(fieldName, []Value{value}, gt) }
func Gte(fieldName string, value Value) Condition { return newCond(fieldName, []Value{value}, gte) }
func Bt(fieldName string, lower, upper Value) Condition {
	return newCond(fieldName, []Value{lower, upper}, bt)
}

func newCond(fieldName string, values []Value, ct conditionType) Condition {
	return Condition{fieldName, values, ct, nil, new(conditionOptions)}
}

type conditionType uint8

const (
	eq conditionType = iota
	neq
	lt
	lte
	gt
	gte
	bt
	all
)

type conditionChildClause struct {
	boolOp boolOp
	cond   Condition
}

func (c conditionChildClause) opString() string {
	switch c.boolOp {
	case and:
		return " and "
	case or:
		return " or "
	default:
		return "" // Special cases
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
