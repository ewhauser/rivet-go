package main

import (
	"encoding/json"
	"testing"
)

func TestDecodeJSONInput(t *testing.T) {
	encoded, err := json.Marshal(inventoryInput{InitialStock: 10, ItemName: "Laptop"})
	if err != nil {
		t.Fatal(err)
	}
	var decoded inventoryInput
	if err := decodeJSONInput(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.InitialStock != 10 || decoded.ItemName != "Laptop" {
		t.Fatalf("decoded inventory = %#v", decoded)
	}
	if err := decodeJSONInput(nil, &decoded); err == nil {
		t.Fatal("empty creation input succeeded")
	}
}

func TestCheckoutWireShapes(t *testing.T) {
	encoded, err := json.Marshal(addItemArgs{ItemID: "laptop", Quantity: 3})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"itemId":"laptop","quantity":3}` {
		t.Fatalf("add-item JSON = %s", encoded)
	}
	result := checkoutSummary{Items: []checkoutItem{{Quantity: 3}}, TotalItems: 3}
	if result.TotalItems != result.Items[0].Quantity {
		t.Fatalf("checkout summary = %#v", result)
	}
}
