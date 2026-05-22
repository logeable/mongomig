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

func TestResolveRestoreHourRange_activeExtendsEnd(t *testing.T) {
	collMeta := &CollectionMeta{
		OldestCompleted: &HourRef{
			Year: 2026, Month: 5, Day: 18, Hour: 0,
			IntervalStartUTC: "2026-05-18T00:00:00Z",
			IntervalEndUTC:   "2026-05-18T01:00:00Z",
		},
		NewestCompleted: &HourRef{
			Year: 2026, Month: 5, Day: 18, Hour: 5,
			IntervalStartUTC: "2026-05-18T05:00:00Z",
			IntervalEndUTC:   "2026-05-18T06:00:00Z",
			Status:           HourStatusComplete,
		},
		Active: &HourRef{
			Year: 2026, Month: 5, Day: 18, Hour: 7,
			IntervalStartUTC: "2026-05-18T07:00:00Z",
			IntervalEndUTC:   "2026-05-18T08:00:00Z",
			Status:           HourStatusPartial,
		},
	}
	_, end, err := resolveRestoreHourRange(HourlyRestoreOpts{}, collMeta, nil)
	if err != nil {
		t.Fatal(err)
	}
	if end.Hour != 7 {
		t.Fatalf("end=%s want hour 7 (active)", end)
	}
}

func TestHourAlreadyRestored(t *testing.T) {
	cp := &RestoreCheckpoint{
		NewestRestored: &HourRef{
			Year: 2026, Month: 1, Day: 1, Hour: 5,
			IntervalStartUTC: "2026-01-01T05:00:00Z",
			IntervalEndUTC:   "2026-01-01T06:00:00Z",
		},
	}
	done := HourBucketUTC(mustParseRFC3339("2026-01-01T05:00:00Z"))
	next := HourBucketUTC(mustParseRFC3339("2026-01-01T06:00:00Z"))
	if !cp.hourAlreadyRestored(done) {
		t.Fatal("hour 5 should be done")
	}
	if cp.hourAlreadyRestored(next) {
		t.Fatal("hour 6 should not be done")
	}
}

func TestSetRestoreActive_recordsHour(t *testing.T) {
	hb := HourBucketUTC(mustParseRFC3339("2026-05-18T07:00:00Z"))
	cp := newRestoreCheckpoint("mongomig", "mongomig/db/coll", "mongodb://localhost", NSSpec{DB: "db", Coll: "coll"})
	cp.setRestoreActive(hb, HourStatusPartial)
	if cp.Active == nil || cp.Active.Hour != 7 || cp.Active.Status != HourStatusPartial {
		t.Fatalf("active=%v", cp.Active)
	}
	if cp.NewestRestored != nil {
		t.Fatal("newest_restored should stay nil for partial-only restore record")
	}
}

func TestMarkHourCompleteAdvancesCursor(t *testing.T) {
	hb := HourBucketUTC(mustParseRFC3339("2026-01-01T07:00:00Z"))
	cp := newRestoreCheckpoint("mongomig", "mongomig/db/coll", "mongodb://localhost", NSSpec{DB: "db", Coll: "coll"})
	cp.markHourComplete(hb)
	if cp.NewestRestored == nil || cp.NewestRestored.Hour != 7 {
		t.Fatalf("newest=%v", cp.NewestRestored)
	}
}

func TestRestoreCheckpointID(t *testing.T) {
	id := restoreCheckpointID("mongomig", "revol", "samples")
	if id != "mongomig/revol/samples" {
		t.Fatalf("got %q", id)
	}
}
