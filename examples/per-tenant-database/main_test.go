package main

import (
	"strings"
	"testing"
	"time"
)

func TestInitializeCompanyStateUsesActorKey(t *testing.T) {
	now := time.UnixMilli(1_234)
	var state companyDatabaseState
	if !initializeCompanyState(&state, "acme", now) {
		t.Fatal("first initialization returned false")
	}
	if state.CompanyName != "acme" || state.CreatedAt != 1_234 || state.UpdatedAt != 1_234 {
		t.Fatalf("initialized state = %#v", state)
	}
	if state.Employees == nil || state.Projects == nil {
		t.Fatal("initialized collections are nil")
	}
	if initializeCompanyState(&state, "changed", now.Add(time.Second)) {
		t.Fatal("existing state was reinitialized")
	}
}

func TestCompanyHelpers(t *testing.T) {
	if got := companyName("  globex  "); got != "globex" {
		t.Fatalf("companyName = %q", got)
	}
	if got := companyName("  "); got != "Unknown Company" {
		t.Fatalf("empty companyName = %q", got)
	}
	for _, status := range []projectStatus{projectPlanning, projectActive, projectDone} {
		if !validProjectStatus(status) {
			t.Fatalf("valid status %q rejected", status)
		}
	}
	if validProjectStatus("paused") {
		t.Fatal("invalid status accepted")
	}
	id, err := newEntityID("emp")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id, "emp_") || len(id) != len("emp_")+12 {
		t.Fatalf("entity ID = %q", id)
	}
}
