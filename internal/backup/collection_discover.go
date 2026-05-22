package backup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// ResolveCollectionSpecs returns namespaces to back up.
// dbName is required. If collectionsCSV is empty, lists all non-system collections in dbName.
// If collectionsCSV is set, each entry may be "coll" or "otherdb.coll" (only the first dot splits).
// excludeCollections drops matching collection names (e.g. restore checkpoint coll on backup).
// log may be nil.
func ResolveCollectionSpecs(ctx context.Context, log *zap.Logger, mongoURI, dbName, collectionsCSV string, excludeCollections ...string) ([]NSSpec, error) {
	dbName = strings.TrimSpace(dbName)
	if dbName == "" {
		return nil, fmt.Errorf("--db is required (mongomig.yaml or --db)")
	}
	collectionsCSV = strings.TrimSpace(collectionsCSV)
	if collectionsCSV == "" {
		if log != nil {
			log.Debug("auto-discover collections in database", zap.String("db", dbName))
		}
		names, err := DiscoverCollectionsInDB(ctx, log, mongoURI, dbName)
		if err != nil {
			return nil, err
		}
		specs := make([]NSSpec, len(names))
		for i, c := range names {
			specs[i] = NSSpec{DB: dbName, Coll: c}
		}
		return filterExcludedSpecs(specs, log, excludeCollections...)
	}
	if log != nil {
		log.Debug("using explicit collection list", zap.String("db", dbName), zap.String("collections", collectionsCSV))
	}
	out, err := parseCollectionsCSV(dbName, collectionsCSV)
	if err != nil {
		return nil, err
	}
	return filterExcludedSpecs(out, log, excludeCollections...)
}

func filterExcludedSpecs(specs []NSSpec, log *zap.Logger, excludeCollections ...string) ([]NSSpec, error) {
	exclude := make(map[string]struct{})
	for _, name := range excludeCollections {
		if s := strings.TrimSpace(name); s != "" {
			exclude[s] = struct{}{}
		}
	}
	if len(exclude) == 0 {
		return specs, nil
	}
	var out []NSSpec
	for _, s := range specs {
		if _, skip := exclude[s.Coll]; skip {
			if log != nil {
				log.Debug("skip excluded collection", zap.String("collection", s.String()))
			}
			continue
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no collections left after exclusions")
	}
	return out, nil
}

// DiscoverCollectionsInDB lists user collection names in db (excludes system.*).
// log may be nil.
func DiscoverCollectionsInDB(ctx context.Context, log *zap.Logger, mongoURI, dbName string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	if log != nil {
		log.Debug("listCollections", zap.String("db", dbName))
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	db := client.Database(dbName)
	cur, err := db.ListCollections(ctx, bson.M{"type": "collection"})
	if err != nil {
		return nil, fmt.Errorf("list collections in %q: %w", dbName, err)
	}
	defer func() { _ = cur.Close(ctx) }()

	var names []string
	for cur.Next(ctx) {
		var doc struct {
			Name string `bson:"name"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		if doc.Name == "" || strings.HasPrefix(doc.Name, "system.") {
			continue
		}
		names = append(names, doc.Name)
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no collections found in database %q", dbName)
	}
	if log != nil {
		log.Debug("collections discovered", zap.String("db", dbName), zap.Strings("names", names), zap.Int("count", len(names)))
	}
	return names, nil
}
