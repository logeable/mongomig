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

func TestIsIndexAlreadyExists(t *testing.T) {
	err := mongo.CommandError{Code: 85, Message: "IndexOptionsConflict"}
	if !isIndexAlreadyExists(err) {
		t.Fatal("expected true for code 85")
	}
}
