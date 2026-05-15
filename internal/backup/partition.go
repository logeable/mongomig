package backup

import (
	"fmt"
	"strings"
	"time"
)

// ParsePartitionCreatedAt parses user input for the OSS path partition "created_at".
// Accepts: YYYY-MM-DD (UTC midnight), RFC3339, or RFC3339Nano.
func ParsePartitionCreatedAt(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("created_at partition is empty")
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.ParseInLocation("2006-01-02", s, time.UTC); err == nil {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
	}
	return time.Time{}, fmt.Errorf("created_at: use YYYY-MM-DD or RFC3339 / RFC3339Nano (UTC)")
}

// PartitionPathSegment turns an instant into a single stable, URL-safe path segment (no slashes).
func PartitionPathSegment(t time.Time) string {
	// UTC wall time, colons removed for path safety: 2026-05-14T08-30-00Z
	u := t.UTC()
	s := u.Format("2006-01-02T15-04-05") + "Z"
	s = strings.ReplaceAll(s, ":", "-")
	return s
}
