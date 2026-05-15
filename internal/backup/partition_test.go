package backup

import (
	"testing"
	"time"
)

func TestParsePartitionCreatedAt_dateOnly(t *testing.T) {
	got, err := ParsePartitionCreatedAt("2026-05-14")
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 5, 14, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if got := PartitionPathSegment(want); got != "2026-05-14T00-00-00Z" {
		t.Fatalf("path seg %q", got)
	}
}

func TestParsePartitionCreatedAt_RFC3339(t *testing.T) {
	got, err := ParsePartitionCreatedAt("2026-05-14T08:30:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if PartitionPathSegment(got) != "2026-05-14T08-30-00Z" {
		t.Fatalf("%s", PartitionPathSegment(got))
	}
}
