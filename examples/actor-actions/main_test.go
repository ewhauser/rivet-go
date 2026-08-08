package main

import (
	"encoding/json"
	"testing"
)

func TestDecodeCreationInput(t *testing.T) {
	encoded, err := json.Marshal(companyInput{Name: "Acme", Industry: "Technology"})
	if err != nil {
		t.Fatal(err)
	}
	var decoded companyInput
	if err := decodeCreationInput(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Name != "Acme" || decoded.Industry != "Technology" {
		t.Fatalf("decoded input = %#v", decoded)
	}
	if err := decodeCreationInput(nil, &decoded); err == nil {
		t.Fatal("empty creation input succeeded")
	}
}

func TestRandomID(t *testing.T) {
	first, err := randomID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := randomID()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 32 || len(second) != 32 || first == second {
		t.Fatalf("random IDs = %q, %q", first, second)
	}
}
