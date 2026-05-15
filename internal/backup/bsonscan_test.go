package backup

import (
	"bytes"
	"encoding/binary"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestReadNextBSONDocument_roundTrip(t *testing.T) {
	ts := primitive.Timestamp{T: 42, I: 3}
	doc, err := bson.Marshal(bson.D{{Key: "ts", Value: ts}})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := buf.Write(doc); err != nil {
		t.Fatal(err)
	}
	got, err := readNextBSONDocument(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		TS primitive.Timestamp `bson:"ts"`
	}
	if err := bson.Unmarshal(got, &out); err != nil {
		t.Fatal(err)
	}
	if out.TS != ts {
		t.Fatalf("ts mismatch: %+v vs %+v", out.TS, ts)
	}
	_, err = readNextBSONDocument(&buf)
	if err == nil {
		t.Fatal("expected error after end of stream")
	}
}

func TestReadNextBSONDocument_invalidSize(t *testing.T) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], 3)
	_, err := readNextBSONDocument(bytes.NewReader(b[:]))
	if err == nil {
		t.Fatal("expected error")
	}
}
