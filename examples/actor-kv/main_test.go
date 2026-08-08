package main

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ewhauser/rivet-go/rivet"
)

func TestByteValues(t *testing.T) {
	got, err := byteValues([]int{0, 1, 127, 255})
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{0, 1, 127, 255}; !reflect.DeepEqual(got, want) {
		t.Fatalf("byteValues = %#v, want %#v", got, want)
	}

	for _, values := range [][]int{{-1}, {256}} {
		_, err := byteValues(values)
		var actionError rivet.ActionError
		if !errors.As(err, &actionError) || actionError.Code != "invalid_byte" {
			t.Fatalf("byteValues(%v) error = %v", values, err)
		}
	}
}

func TestValidateKVKey(t *testing.T) {
	if err := validateKVKey("greeting:user"); err != nil {
		t.Fatalf("valid key: %v", err)
	}
	if err := validateKVKey("  "); err == nil {
		t.Fatal("blank key accepted")
	}
}
