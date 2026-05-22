package backup

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestIndexKeyPairsFromD_order(t *testing.T) {
	d := bson.D{
		{Key: "tenant_key", Value: 1},
		{Key: "live_room_id", Value: 1},
		{Key: "created_at", Value: -1},
	}
	pairs, err := indexKeyPairsFromD(d)
	if err != nil || len(pairs) != 3 {
		t.Fatalf("pairs=%v err=%v", pairs, err)
	}
	if pairs[0].Field != "tenant_key" || pairs[2].Field != "created_at" {
		t.Fatalf("%+v", pairs)
	}
}

func TestBsonDFromIndexKeyPairs_roundTrip(t *testing.T) {
	pairs, err := indexKeyPairsFromD(bson.D{
		{Key: "tenant_key", Value: 1},
		{Key: "live_room_id", Value: 1},
		{Key: "created_at", Value: -1},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := bsonDFromIndexKeyPairs(pairs)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Key != "tenant_key" || got[2].Key != "created_at" {
		t.Fatalf("%+v", got)
	}
}

func TestIndexModelFromCatalogEntry_compound(t *testing.T) {
	pairs, _ := indexKeyPairsFromD(bson.D{
		{Key: "tenant_key", Value: 1},
		{Key: "live_room_id", Value: 1},
		{Key: "created_at", Value: -1},
	})
	entry := indexCatalogEntry{Name: "tenant_key_1_live_room_id_1_created_at_-1", Key: pairs}
	m, ok, err := indexModelFromCatalogEntry(entry)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if _, isD := m.Keys.(bson.D); !isD {
		t.Fatalf("keys type %T want bson.D", m.Keys)
	}
}

func TestIndexCatalogEntryFromBSONRaw(t *testing.T) {
	raw, err := bson.Marshal(bson.M{
		"v": 2, "name": "x", "unique": true,
		"key": bson.D{{Key: "tenant_key", Value: 1}, {Key: "live_room_id", Value: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := indexCatalogEntryFromBSONRaw(raw)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Name != "x" || len(entry.Key) != 2 || entry.Key[0].Field != "tenant_key" {
		t.Fatalf("%+v", entry)
	}
	d, err := bsonDFromIndexKeyPairs(entry.Key)
	if err != nil || len(d) != 2 || d[0].Key != "tenant_key" {
		t.Fatalf("key=%+v err=%v", d, err)
	}
}
