package backup

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestIndexModelFromCatalogDoc_skipsID(t *testing.T) {
	_, ok, err := indexModelFromCatalogDoc(bson.M{"name": "_id_", "key": bson.M{"_id": 1}})
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestIndexModelFromCatalogDoc_unique(t *testing.T) {
	m, ok, err := indexModelFromCatalogDoc(bson.M{
		"name":   "tenant_key_1",
		"key":    bson.M{"tenant_key": 1},
		"unique": true,
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
