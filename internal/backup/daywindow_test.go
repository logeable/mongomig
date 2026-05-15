package backup

import (
	"bytes"
	"os"
	"testing"
	"time"
)

func TestCivilDayRangeUTC_Shanghai(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	start, end, err := CivilDayRangeUTC("2026-05-15", loc)
	if err != nil {
		t.Fatal(err)
	}
	if g, w := start.Format(time.RFC3339), "2026-05-14T16:00:00Z"; g != w {
		t.Fatalf("start UTC: got %s want %s", g, w)
	}
	if g, w := end.Sub(start), 24*time.Hour; g != w {
		t.Fatalf("range width: got %v want %v", g, w)
	}
}

func TestCivilDaysInclusive(t *testing.T) {
	loc := time.UTC
	a := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	b := time.Date(2026, 5, 3, 8, 0, 0, 0, time.UTC)
	days, err := CivilDaysInclusive(a, b, loc)
	if err != nil {
		t.Fatal(err)
	}
	if len(days) != 3 {
		t.Fatalf("len=%d %+v", len(days), days)
	}
	if days[0] != "2026-05-01" || days[2] != "2026-05-03" {
		t.Fatalf("%+v", days)
	}
}

func TestWriteTenantDayRangeQueryFile(t *testing.T) {
	dir := t.TempDir()
	p := dir + "/q.json"
	start := time.Date(2026, 5, 14, 16, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 15, 16, 0, 0, 0, time.UTC)
	if err := WriteTenantDayRangeQueryFile(p, "tenant_key", "t1", false, "created_at", start, end); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"tenant_key":"t1"`)) {
		t.Fatalf("%s", raw)
	}
	if !bytes.Contains(raw, []byte(`2026-05-14T16:00:00.000Z`)) {
		t.Fatalf("%s", raw)
	}
}
