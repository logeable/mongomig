package backup

import (
	"testing"
	"time"
)

func TestTenantShard(t *testing.T) {
	sh, err := TenantShard("isolation_1eff7eac34924bd390ead2f9431f04bd")
	if err != nil {
		t.Fatal(err)
	}
	if sh != "1e" {
		t.Fatalf("shard=%q want 1e", sh)
	}
}

func TestTenantShard_errors(t *testing.T) {
	if _, err := TenantShard("nounderscore"); err == nil {
		t.Fatal("expected error without underscore")
	}
	if _, err := TenantShard("isolation_x"); err == nil {
		t.Fatal("expected error for short suffix")
	}
}

func TestTenantDataRelPath(t *testing.T) {
	got := TenantDataRelPath("1e", "isolation_abc")
	want := "1e/isolation_abc/dump.tar"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestHourBucketUTC_andHoursInclusive(t *testing.T) {
	start := HourBucketUTC(time.Date(2026, 5, 15, 17, 30, 0, 0, time.UTC))
	if start.Hour != 17 {
		t.Fatalf("hour=%d", start.Hour)
	}
	end := start
	hours := HoursInclusive(start, end)
	if len(hours) != 1 {
		t.Fatalf("len=%d", len(hours))
	}
	next := start.Next()
	if next.Hour != 18 {
		t.Fatalf("next hour=%d", next.Hour)
	}
}

func TestParseHourFlag(t *testing.T) {
	b, err := ParseHourFlag("2026-05-15T17")
	if err != nil {
		t.Fatal(err)
	}
	if b.Year != 2026 || b.Month != 5 || b.Day != 15 || b.Hour != 17 {
		t.Fatalf("%+v", b)
	}
}
