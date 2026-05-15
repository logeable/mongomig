package backup

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// MaxOplogTSInDump scans a mongodump-produced BSON file (oplog.rs or oplog.bson) for max ts.
func MaxOplogTSInDump(path string, gz bool) (primitive.Timestamp, error) {
	f, err := os.Open(path)
	if err != nil {
		return primitive.Timestamp{}, err
	}
	defer f.Close()

	var r io.Reader = f
	if gz {
		gzr, err := gzip.NewReader(f)
		if err != nil {
			return primitive.Timestamp{}, err
		}
		defer gzr.Close()
		r = gzr
	}

	var max primitive.Timestamp
	n := 0
	for {
		raw, err := readNextBSONDocument(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return primitive.Timestamp{}, err
		}
		var doc struct {
			TS primitive.Timestamp `bson:"ts"`
		}
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return primitive.Timestamp{}, err
		}
		if tsAfter(doc.TS, max) {
			max = doc.TS
		}
		n++
	}
	if n == 0 {
		return primitive.Timestamp{}, fmt.Errorf("no oplog documents in %s", path)
	}
	return max, nil
}

func readNextBSONDocument(r io.Reader) ([]byte, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return nil, err
	}
	sz := int32(binary.LittleEndian.Uint32(sizeBuf[:]))
	if sz < 5 {
		return nil, fmt.Errorf("invalid bson size %d", sz)
	}
	// Reasonable upper bound to avoid OOM on corrupt input (256 MiB).
	if sz > 1<<28 {
		return nil, fmt.Errorf("bson size too large: %d", sz)
	}
	doc := make([]byte, sz)
	copy(doc, sizeBuf[:])
	if _, err := io.ReadFull(r, doc[4:]); err != nil {
		return nil, err
	}
	return doc, nil
}
