package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	"github.com/logeable/mongomig/internal/config"
)

func (s *MetaStore) LoadCollectionIndexes(ctx context.Context, collectionBase string) (*collectionIndexesFile, bool, error) {
	var cat collectionIndexesFile
	ok, err := s.loadJSON(ctx, CollectionIndexesKey(collectionBase), &cat)
	if err != nil || !ok {
		return nil, ok, err
	}
	return &cat, true, nil
}

// SyncCollectionIndexesFromOSS aligns MongoDB indexes with OSS indexes.json (source of truth).
// Called once per collection per restore run: creates missing indexes and drops local indexes not in OSS.
func SyncCollectionIndexesFromOSS(ctx context.Context, mongoURI string, meta *MetaStore, log *zap.Logger, ns NSSpec, collectionBase string) error {
	cat, ok, err := meta.LoadCollectionIndexes(ctx, collectionBase)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: indexes.json missing on OSS at %s (run mongomig backup first)", ns.String(), CollectionIndexesKey(collectionBase))
	}
	if cat.DB != "" && cat.DB != ns.DB {
		return fmt.Errorf("indexes.json db mismatch: catalog %s, restore %s", cat.DB, ns.DB)
	}
	if cat.Collection != "" && cat.Collection != ns.Coll {
		return fmt.Errorf("indexes.json collection mismatch: catalog %s, restore %s", cat.Collection, ns.Coll)
	}
	return syncCollectionIndexes(ctx, mongoURI, log, ns, cat)
}

func syncCollectionIndexes(ctx context.Context, mongoURI string, log *zap.Logger, ns NSSpec, cat *collectionIndexesFile) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("connect for index sync: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database(ns.DB).Collection(ns.Coll)
	desired := make(map[string]mongo.IndexModel)
	for _, entry := range cat.Indexes {
		model, ok, err := indexModelFromCatalogEntry(entry)
		if err != nil {
			return fmt.Errorf("%s: %w", ns.String(), err)
		}
		if !ok {
			continue
		}
		name := indexModelName(model)
		if name == "" {
			return fmt.Errorf("%s: index from OSS indexes.json has no name", ns.String())
		}
		desired[name] = model
	}

	local, err := listCollectionIndexNames(ctx, coll)
	if err != nil {
		return err
	}
	dropped := 0
	created := 0
	kept := 0
	for _, name := range local {
		if name == "_id_" {
			continue
		}
		if _, want := desired[name]; want {
			kept++
			continue
		}
		if _, err := coll.Indexes().DropOne(ctx, name); err != nil {
			return fmt.Errorf("%s drop index %q: %w", ns.String(), name, err)
		}
		dropped++
		if log != nil {
			log.Info("dropped index not on OSS", zap.String("collection", ns.String()), zap.String("index", name))
		}
	}
	for name, model := range desired {
		_, err := coll.Indexes().CreateOne(ctx, model)
		if err == nil {
			created++
			if log != nil {
				log.Debug("index created from OSS", zap.String("collection", ns.String()), zap.String("index", name))
			}
			continue
		}
		if isIndexAlreadyExists(err) {
			kept++
			continue
		}
		return fmt.Errorf("%s create index %q: %w", ns.String(), name, err)
	}
	if log != nil {
		log.Info("collection indexes synced from OSS",
			zap.String("collection", ns.String()),
			zap.Int("created", created),
			zap.Int("dropped", dropped),
			zap.Int("unchanged", kept),
			zap.Int("oss_index_count", len(desired)),
		)
	}
	return nil
}

func listCollectionIndexNames(ctx context.Context, coll *mongo.Collection) ([]string, error) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		if isNamespaceNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()
	var names []string
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		name, _ := doc["name"].(string)
		if name != "" {
			names = append(names, name)
		}
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

func indexModelName(model mongo.IndexModel) string {
	if model.Options == nil || model.Options.Name == nil {
		return ""
	}
	return *model.Options.Name
}

func isNamespaceNotFound(err error) bool {
	if err == nil {
		return false
	}
	var cmd mongo.CommandError
	if errors.As(err, &cmd) {
		return cmd.Code == 26 // NamespaceNotFound
	}
	return false
}

func isIndexAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	var cmd mongo.CommandError
	if errors.As(err, &cmd) {
		switch cmd.Code {
		case 68, 85, 86:
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exist") ||
		strings.Contains(msg, "indexoptionsconflict") ||
		strings.Contains(msg, "indexkeyspecsconflict")
}

// deferIndexSyncUntilAfterDrop reports whether the first mongorestore uses --drop on an empty reload.
func deferIndexSyncUntilAfterDrop(opts HourlyRestoreOpts, cp *RestoreCheckpoint) bool {
	if !opts.Drop {
		return false
	}
	return cp == nil || cp.NewestRestored == nil
}

func syncCollectionIndexesOnce(
	ctx context.Context,
	cfg *config.Root,
	meta *MetaStore,
	log *zap.Logger,
	ns NSSpec,
	collBase string,
	synced map[string]bool,
) error {
	key := ns.String()
	if synced[key] {
		return nil
	}
	if err := SyncCollectionIndexesFromOSS(ctx, cfg.MongoURI, meta, log, ns, collBase); err != nil {
		return err
	}
	synced[key] = true
	return nil
}
