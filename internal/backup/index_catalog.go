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
)

// WriteIndexCatalog writes one JSON file per collection listing index definitions (MongoDB catalog).
// Stored under destDir/_indexes/ alongside mongodump *.metadata.json for redundancy and human review.
func WriteIndexCatalog(ctx context.Context, mongoURI, destDir string, specs []NSSpec) error {
	idxRoot := filepath.Join(destDir, "_indexes")
	if err := os.MkdirAll(idxRoot, 0o750); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("connect for index catalog: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	for _, ns := range specs {
		coll := client.Database(ns.DB).Collection(ns.Coll)
		cur, err := coll.Indexes().List(ctx)
		if err != nil {
			return fmt.Errorf("list indexes %s.%s: %w", ns.DB, ns.Coll, err)
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

		name := fmt.Sprintf("%s__%s.json", strings.Map(safePathRune, ns.DB), strings.Map(safePathRune, ns.Coll))
		data, err := json.MarshalIndent(docs, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(idxRoot, name), data, 0o640); err != nil {
			return err
		}
	}
	return nil
}
