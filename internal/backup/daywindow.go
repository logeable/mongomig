package backup

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var timeFieldNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ValidateBSONFieldName returns an error if s is not safe for use as a MongoDB field path segment.
func ValidateBSONFieldName(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return fmt.Errorf("field name is empty")
	}
	if !timeFieldNameRe.MatchString(s) {
		return fmt.Errorf("invalid field name %q (use letters, digits, underscore)", s)
	}
	return nil
}

// LoadIANATimezone loads a location by name (e.g. Asia/Shanghai). Empty defaults to UTC.
func LoadIANATimezone(name string) (*time.Location, error) {
	name = strings.TrimSpace(name)
	if name == "" || name == "UTC" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("timezone %q: %w", name, err)
	}
	return loc, nil
}

// CivilMidnightInTZ returns midnight on the given civil calendar date in loc (wall clock).
func CivilMidnightInTZ(civilDay string, loc *time.Location) (time.Time, error) {
	civilDay = strings.TrimSpace(civilDay)
	if civilDay == "" {
		return time.Time{}, fmt.Errorf("civil day is empty")
	}
	t, err := time.ParseInLocation("2006-01-02", civilDay, loc)
	if err != nil {
		return time.Time{}, fmt.Errorf("civil day %q: use YYYY-MM-DD", civilDay)
	}
	return t, nil
}

// CivilDayRangeUTC returns [startUTC, endUTC) for the civil calendar day in loc.
func CivilDayRangeUTC(civilDay string, loc *time.Location) (startUTC, endUTC time.Time, err error) {
	start, err := CivilMidnightInTZ(civilDay, loc)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return start.UTC(), start.AddDate(0, 0, 1).UTC(), nil
}

// TruncateToCivilMidnightInTZ converts instant t to midnight of that civil date in loc.
func TruncateToCivilMidnightInTZ(t time.Time, loc *time.Location) time.Time {
	x := t.In(loc)
	return time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, loc)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// CivilDaysInclusive returns each civil calendar date (YYYY-MM-DD in loc) from start through end inclusive,
// where start/end are instants interpreted in loc and truncated to civil midnights.
func CivilDaysInclusive(start, end time.Time, loc *time.Location) ([]string, error) {
	if loc == nil {
		loc = time.UTC
	}
	ds := TruncateToCivilMidnightInTZ(start, loc)
	de := TruncateToCivilMidnightInTZ(end, loc)
	if de.Before(ds) {
		return nil, fmt.Errorf("end day before start day")
	}
	var out []string
	for d := ds; !d.After(de); d = d.AddDate(0, 0, 1) {
		out = append(out, d.Format("2006-01-02"))
	}
	return out, nil
}
