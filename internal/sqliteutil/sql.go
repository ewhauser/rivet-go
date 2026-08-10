package sqliteutil

import "strings"

// DefinitelyReadOnly identifies the deliberately small subset of SQL that is
// safe to abandon after transport admission. Everything uncertain is treated
// as potentially mutating so a caller never receives a retryable context error
// while the original statement can still commit.
func DefinitelyReadOnly(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	if len(trimmed) < len("SELECT") || !strings.EqualFold(trimmed[:len("SELECT")], "SELECT") {
		return false
	}
	if len(trimmed) == len("SELECT") {
		return true
	}
	return trimmed[len("SELECT")] == ' ' || trimmed[len("SELECT")] == '\t' ||
		trimmed[len("SELECT")] == '\n' || trimmed[len("SELECT")] == '\r' ||
		trimmed[len("SELECT")] == '\f'
}
