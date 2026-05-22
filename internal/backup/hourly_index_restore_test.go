package backup

import (
	"encoding/json"
	"testing"

	"go.mongodb.org/mongo-driver/mongo"
)

func TestIndexModelFromCatalogEntry_skipsID(t *testing.T) {
	_, ok, err := indexModelFromCatalogEntry(indexCatalogEntry{
		Name: "_id_",
		Key:  []indexKeyPair{{Field: "_id", Value: json.RawMessage(`1`)}},
	})
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestIndexModelFromCatalogEntry_unique(t *testing.T) {
	u := true
	m, ok, err := indexModelFromCatalogEntry(indexCatalogEntry{
		Name:   "tenant_key_1",
		Key:    []indexKeyPair{{Field: "tenant_key", Value: json.RawMessage(`1`)}},
		Unique: &u,
	})
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if m.Options == nil || m.Options.Unique == nil || !*m.Options.Unique {
		t.Fatalf("unique not set: %+v", m.Options)
	}
}

func TestDeferIndexSyncUntilAfterDrop(t *testing.T) {
	if !deferIndexSyncUntilAfterDrop(HourlyRestoreOpts{Drop: true}, nil) {
		t.Fatal("expected defer when drop and no checkpoint progress")
	}
	if deferIndexSyncUntilAfterDrop(HourlyRestoreOpts{Drop: false}, nil) {
		t.Fatal("expected no defer without drop")
	}
	if deferIndexSyncUntilAfterDrop(HourlyRestoreOpts{Drop: true}, &RestoreCheckpoint{
		NewestRestored: &HourRef{Year: 2026, Month: 1, Day: 1, Hour: 0, IntervalStartUTC: "2026-01-01T00:00:00Z", IntervalEndUTC: "2026-01-01T01:00:00Z"},
	}) {
		t.Fatal("expected no defer when checkpoint has newest_restored")
	}
}

func TestIsIndexAlreadyExists(t *testing.T) {
	err := mongo.CommandError{Code: 85, Message: "IndexOptionsConflict"}
	if !isIndexAlreadyExists(err) {
		t.Fatal("expected true for code 85")
	}
}
