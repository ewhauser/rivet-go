package rivet

import "testing"

func TestResolveSQLiteTransportDefaultsToFFI(t *testing.T) {
	if got := resolveSQLiteTransport("", ""); got != SQLiteTransportFFI {
		t.Fatalf("default transport = %q, want ffi", got)
	}
	if got := resolveSQLiteTransport(SQLiteTransportSocket, ""); got != SQLiteTransportSocket {
		t.Fatalf("configured transport = %q, want socket", got)
	}
	if got := resolveSQLiteTransport(SQLiteTransportSocket, "disabled"); got != SQLiteTransportDisabled {
		t.Fatalf("env override = %q, want disabled", got)
	}
	if got := resolveSQLiteTransport("", "socket"); got != SQLiteTransportSocket {
		t.Fatalf("env-only transport = %q, want socket", got)
	}
}

func TestValidateAndWireSQLiteTransport(t *testing.T) {
	for _, transport := range []SQLiteTransport{SQLiteTransportFFI, SQLiteTransportSocket, SQLiteTransportDisabled} {
		if err := validateSQLiteTransport(transport); err != nil {
			t.Fatalf("valid transport %q rejected: %v", transport, err)
		}
	}
	if err := validateSQLiteTransport(""); err == nil {
		t.Fatal("empty transport must be rejected after resolution")
	}
	if err := validateSQLiteTransport("bogus"); err == nil {
		t.Fatal("bogus transport must be rejected")
	}
	if got := wireSQLiteTransport(SQLiteTransportDisabled); got != "" {
		t.Fatalf("disabled wire encoding = %q, want empty", got)
	}
	if got := wireSQLiteTransport(SQLiteTransportFFI); got != "ffi" {
		t.Fatalf("ffi wire encoding = %q", got)
	}
}
