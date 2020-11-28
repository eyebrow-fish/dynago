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

func Test_projOf_arrays(t *testing.T) {
	type Dog struct {
		Name string
	}
	type Store struct {
		Names   []string
		Buddies []Dog
	}

	p := projOf(Store{})

	ex := "Names,Buddies.Name"
	if p != ex {
		t.Fatalf("expected %v, got %v", ex, p)
	}
}
