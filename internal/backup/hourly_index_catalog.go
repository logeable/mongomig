package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/logeable/mongomig/internal/storage"
)

// WriteCollectionIndexesJSON uploads indexes.json at collection prefix (overwrites).
func WriteCollectionIndexesJSON(ctx context.Context, mongoURI string, remote storage.Backend, meta *MetaStore, collectionBase string, ns NSSpec) error {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("connect for indexes: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database(ns.DB).Collection(ns.Coll)
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list indexes %s: %w", ns.String(), err)
	}
	var docs []bson.M
	for cur.Next(ctx) {
		var m bson.M
		if err := cur.Decode(&m); err != nil {
			_ = cur.Close(ctx)
			return err
		}
		docs = append(docs, m)
	}
	if err := cur.Err(); err != nil {
		_ = cur.Close(ctx)
		return err
	}
	_ = cur.Close(ctx)

	payload := map[string]any{
		"db":         ns.DB,
		"collection": ns.Coll,
		"indexes":    docs,
		"updated_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	local := filepath.Join(meta.tmpDir, "indexes-"+safePathSegment(ns.DB)+"_"+safePathSegment(ns.Coll)+".json")
	if err := os.WriteFile(local, data, 0o640); err != nil {
		return err
	}
	return remote.UploadFile(ctx, local, CollectionIndexesKey(collectionBase))
}

func safePathSegment(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, s)
}
