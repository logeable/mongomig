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

// CollectionIndexesCatalog is the JSON shape of OSS indexes.json (see WriteCollectionIndexesJSON).
type CollectionIndexesCatalog struct {
	DB         string   `json:"db"`
	Collection string   `json:"collection"`
	Indexes    []bson.M `json:"indexes"`
	UpdatedAt  string   `json:"updated_at"`
}

func (s *MetaStore) LoadCollectionIndexes(ctx context.Context, collectionBase string) (*CollectionIndexesCatalog, bool, error) {
	var cat CollectionIndexesCatalog
	ok, err := s.loadJSON(ctx, CollectionIndexesKey(collectionBase), &cat)
	if err != nil || !ok {
		return nil, ok, err
	}
	return &cat, true, nil
}

// EnsureCollectionIndexesFromOSS downloads indexes.json and creates missing indexes on the restore target.
// Call once per collection after the first successful tenant mongorestore (required when --drop clears indexes).
func EnsureCollectionIndexesFromOSS(ctx context.Context, mongoURI string, meta *MetaStore, log *zap.Logger, ns NSSpec, collectionBase string) error {
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
	return EnsureCollectionIndexes(ctx, mongoURI, log, ns, cat)
}

func EnsureCollectionIndexes(ctx context.Context, mongoURI string, log *zap.Logger, ns NSSpec, cat *CollectionIndexesCatalog) error {
	if cat == nil || len(cat.Indexes) == 0 {
		if log != nil {
			log.Warn("indexes.json has no indexes", zap.String("collection", ns.String()))
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("connect for index restore: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database(ns.DB).Collection(ns.Coll)
	created := 0
	skipped := 0
	for _, doc := range cat.Indexes {
		model, ok, err := indexModelFromCatalogDoc(doc)
		if err != nil {
			return fmt.Errorf("%s: %w", ns.String(), err)
		}
		if !ok {
			skipped++
			continue
		}
		name := ""
		if model.Options != nil && model.Options.Name != nil {
			name = *model.Options.Name
		}
		_, err = coll.Indexes().CreateOne(ctx, model)
		if err == nil {
			created++
			if log != nil {
				log.Debug("index created", zap.String("collection", ns.String()), zap.String("index", name))
			}
			continue
		}
		if isIndexAlreadyExists(err) {
			skipped++
			continue
		}
		return fmt.Errorf("%s create index %q: %w", ns.String(), name, err)
	}
	if log != nil {
		log.Info("collection indexes ensured",
			zap.String("collection", ns.String()),
			zap.Int("created", created),
			zap.Int("skipped", skipped),
		)
	}
	return nil
}

func indexModelFromCatalogDoc(doc bson.M) (mongo.IndexModel, bool, error) {
	name, _ := doc["name"].(string)
	if name == "_id_" {
		return mongo.IndexModel{}, false, nil
	}
	key := doc["key"]
	if key == nil {
		return mongo.IndexModel{}, false, fmt.Errorf("index %q missing key", name)
	}
	opts := options.Index()
	if name != "" {
		opts.SetName(name)
	}
	if v, ok := doc["unique"].(bool); ok && v {
		opts.SetUnique(true)
	}
	if v, ok := doc["sparse"].(bool); ok && v {
		opts.SetSparse(true)
	}
	if v, ok := doc["hidden"].(bool); ok && v {
		opts.SetHidden(true)
	}
	if v, ok := doc["expireAfterSeconds"].(int32); ok {
		opts.SetExpireAfterSeconds(v)
	} else if v, ok := doc["expireAfterSeconds"].(int64); ok {
		opts.SetExpireAfterSeconds(int32(v))
	} else if v, ok := doc["expireAfterSeconds"].(float64); ok {
		opts.SetExpireAfterSeconds(int32(v))
	}
	if v, ok := doc["partialFilterExpression"]; ok && v != nil {
		opts.SetPartialFilterExpression(v)
	}
	if v, ok := doc["collation"]; ok && v != nil {
		collation, err := indexCollationFromBSON(v)
		if err != nil {
			return mongo.IndexModel{}, false, err
		}
		opts.SetCollation(collation)
	}
	if v, ok := doc["weights"]; ok && v != nil {
		if w, ok := v.(bson.M); ok {
			opts.SetWeights(w)
		}
	}
	if v, ok := doc["default_language"].(string); ok {
		opts.SetDefaultLanguage(v)
	}
	if v, ok := doc["language_override"].(string); ok {
		opts.SetLanguageOverride(v)
	}
	if v, ok := doc["textIndexVersion"].(int32); ok {
		opts.SetTextVersion(v)
	}
	return mongo.IndexModel{Keys: key, Options: opts}, true, nil
}

func indexCollationFromBSON(v any) (*options.Collation, error) {
	raw, err := bson.Marshal(v)
	if err != nil {
		return nil, err
	}
	var c options.Collation
	if err := bson.Unmarshal(raw, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func isIndexAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	var cmd mongo.CommandError
	if errors.As(err, &cmd) {
		switch cmd.Code {
		case 68, 85, 86: // IndexAlreadyExists, IndexOptionsConflict, IndexKeySpecsConflict
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exist") ||
		strings.Contains(msg, "indexoptionsconflict") ||
		strings.Contains(msg, "indexkeyspecsconflict")
}

func ensureCollectionIndexesOnce(
	ctx context.Context,
	cfg *config.Root,
	meta *MetaStore,
	log *zap.Logger,
	ns NSSpec,
	collBase string,
	indexesEnsured map[string]bool,
) error {
	key := ns.String()
	if indexesEnsured[key] {
		return nil
	}
	if err := EnsureCollectionIndexesFromOSS(ctx, cfg.MongoURI, meta, log, ns, collBase); err != nil {
		return err
	}
	indexesEnsured[key] = true
	return nil
}
