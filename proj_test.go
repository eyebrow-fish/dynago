package dynago

import "testing"

func Test_projOf_simpleWithNested(t *testing.T) {
	type Dog struct {
		Name string
	}
	type House struct {
		Number uint
		Street string
		Owner  Dog
	}

	p := projOf(House{})

	ex := "Number,Street,Owner.Name"
	if p != ex {
		t.Fatalf("expected %v, got %v", ex, p)
	}
}
