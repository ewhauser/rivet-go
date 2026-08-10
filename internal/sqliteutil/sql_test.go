package sqliteutil

import "testing"

func TestDefinitelyReadOnlyIsConservative(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{sql: "SELECT 1", want: true},
		{sql: "\nselect\t* FROM todos", want: true},
		{sql: "WITH todo AS (SELECT 1) SELECT * FROM todo"},
		{sql: "WITH todo AS (SELECT 1) INSERT INTO todos SELECT * FROM todo"},
		{sql: "PRAGMA user_version"},
		{sql: "EXPLAIN SELECT 1"},
		{sql: "SELECT/* generated */ 1"},
		{sql: "SELECTED"},
		{sql: "INSERT INTO todos(label) VALUES ('x')"},
	}
	for _, test := range tests {
		if got := DefinitelyReadOnly(test.sql); got != test.want {
			t.Errorf("DefinitelyReadOnly(%q) = %v, want %v", test.sql, got, test.want)
		}
	}
}
