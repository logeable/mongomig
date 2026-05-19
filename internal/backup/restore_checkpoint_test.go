package backup

import (
	"testing"
)

func TestResolveRestoreHourRange_checkpointIncremental(t *testing.T) {
	cp := &RestoreCheckpoint{
		NewestRestored: &HourRef{
			Year: 2026, Month: 1, Day: 1, Hour: 5,
			IntervalStartUTC: "2026-01-01T05:00:00Z",
			IntervalEndUTC:   "2026-01-01T06:00:00Z",
		},
	}
	collMeta := &CollectionMeta{
		OldestCompleted: &HourRef{Year: 2026, Month: 1, Day: 1, Hour: 0, IntervalStartUTC: "2026-01-01T00:00:00Z", IntervalEndUTC: "2026-01-01T01:00:00Z"},
		NewestCompleted: &HourRef{Year: 2026, Month: 1, Day: 1, Hour: 8, IntervalStartUTC: "2026-01-01T08:00:00Z", IntervalEndUTC: "2026-01-01T09:00:00Z"},
	}
	start, end, err := resolveRestoreHourRange(HourlyRestoreOpts{}, collMeta, cp)
	if err != nil {
		t.Fatal(err)
	}
	if start.Hour != 6 || end.Hour != 8 {
		t.Fatalf("got %s..%s want hour 6..8", start, end)
	}
}

func TestCheckpointTenantAndHour(t *testing.T) {
	hb := HourBucketUTC(mustParseRFC3339("2026-01-01T01:00:00Z"))
	cp := newRestoreCheckpoint("mongomig", "mongomig/db/coll", "mongodb://localhost", NSSpec{DB: "db", Coll: "coll"})
	row := TenantMetaRow{TenantKey: "t1", DataRelPath: "ab/t1/dump.tar", SHA256: "abc"}
	cp.markTenantRestored(hb, row)
	if !cp.tenantRestoredInHour(hb, row) {
		t.Fatal("expected tenant restored")
	}
	cp.markHourComplete(hb)
	tenants := []TenantMetaRow{row}
	if !cp.hourFullyRestored(hb, tenants) {
		t.Fatal("expected hour complete")
	}
}

func TestRestoreCheckpointID(t *testing.T) {
	id := restoreCheckpointID("mongomig", "revol", "samples")
	if id != "mongomig/revol/samples" {
		t.Fatalf("got %q", id)
	}
}
