package backup

import (
	"testing"
)

func TestResolveRestoreHourRange_flags(t *testing.T) {
	from := HourBucketUTC(mustParseRFC3339("2026-01-01T00:00:00Z"))
	to := HourBucketUTC(mustParseRFC3339("2026-01-02T00:00:00Z"))
	start, end, err := resolveRestoreHourRange(HourlyRestoreOpts{FromHour: &from, ToHour: &to}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if start.String() != from.String() || end.String() != to.String() {
		t.Fatalf("got %s..%s want %s..%s", start, end, from, to)
	}
}

func TestResolveRestoreHourRange_meta(t *testing.T) {
	cm := &CollectionMeta{
		OldestCompleted: &HourRef{Year: 2026, Month: 1, Day: 1, Hour: 0, IntervalStartUTC: "2026-01-01T00:00:00Z", IntervalEndUTC: "2026-01-01T01:00:00Z"},
		NewestCompleted: &HourRef{Year: 2026, Month: 1, Day: 1, Hour: 2, IntervalStartUTC: "2026-01-01T02:00:00Z", IntervalEndUTC: "2026-01-01T03:00:00Z"},
	}
	start, end, err := resolveRestoreHourRange(HourlyRestoreOpts{}, cm, nil)
	if err != nil {
		t.Fatal(err)
	}
	if start.Hour != 0 || end.Hour != 2 {
		t.Fatalf("got %s..%s", start, end)
	}
}

func TestTenantsToRestore(t *testing.T) {
	rows := []TenantMetaRow{
		{TenantKey: "a", Uploaded: true, DataRelPath: "1e/a/dump.tar"},
		{TenantKey: "b", Uploaded: false, DataRelPath: "1e/b/dump.tar"},
		{TenantKey: "c", Uploaded: true, Error: "fail", DataRelPath: "1e/c/dump.tar"},
		{TenantKey: "d", Uploaded: true, DataRelPath: ""},
	}
	got := tenantsToRestore(rows)
	if len(got) != 1 || got[0].TenantKey != "a" {
		t.Fatalf("got %+v", got)
	}
}
