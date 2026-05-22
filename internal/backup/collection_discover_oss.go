package backup

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/logeable/mongomig/internal/storage"
	"go.uber.org/zap"
)

// dbOSSPrefix is {remotePrefix}/{db}/ with trailing slash.
func dbOSSPrefix(remotePrefix, dbName string) string {
	return CollectionBase(remotePrefix, dbName, "") + "/"
}

// collectionNamesFromOSSKeys returns collection names that have {dbPrefix}{coll}/meta.json on OSS.
func collectionNamesFromOSSKeys(dbPrefix string, keys []string) []string {
	dbPrefix = strings.TrimPrefix(strings.TrimSuffix(dbPrefix, "/")+"/", "/")
	seen := make(map[string]struct{})
	var names []string
	for _, key := range keys {
		key = strings.TrimPrefix(key, "/")
		if !strings.HasPrefix(key, dbPrefix) {
			continue
		}
		rel := strings.TrimPrefix(key, dbPrefix)
		if !strings.HasSuffix(rel, "/meta.json") {
			continue
		}
		coll := strings.TrimSuffix(rel, "/meta.json")
		if coll == "" || strings.Contains(coll, "/") {
			continue
		}
		if _, ok := seen[coll]; ok {
			continue
		}
		seen[coll] = struct{}{}
		names = append(names, coll)
	}
	sort.Strings(names)
	return names
}

// DiscoverCollectionsOnOSS lists collections under {remotePrefix}/{db}/ that have collection meta.json on OSS.
func DiscoverCollectionsOnOSS(ctx context.Context, log *zap.Logger, remote storage.Backend, remotePrefix, dbName string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	prefix := dbOSSPrefix(remotePrefix, dbName)
	if log != nil {
		log.Debug("list OSS keys for restore collection discover", zap.String("prefix", prefix))
	}
	keys, err := remote.ListKeys(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list OSS keys under %q: %w", prefix, err)
	}
	names := collectionNamesFromOSSKeys(prefix, keys)
	if len(names) == 0 {
		return nil, fmt.Errorf("no collections with meta.json under OSS prefix %q (run mongomig backup first)", strings.TrimSuffix(prefix, "/"))
	}
	if log != nil {
		log.Debug("collections discovered on OSS", zap.String("db", dbName), zap.Strings("names", names), zap.Int("count", len(names)))
	}
	return names, nil
}

func collectionMetaOnOSS(keys []string, collBase string) bool {
	want := CollectionMetaKey(collBase)
	for _, k := range keys {
		if k == want {
			return true
		}
	}
	return false
}

// ResolveRestoreCollectionSpecs resolves collections to restore from OSS layout (not MongoDB listCollections).
// Empty collectionsCSV discovers all {remotePrefix}/{db}/{coll}/meta.json; explicit names must exist on OSS.
func ResolveRestoreCollectionSpecs(ctx context.Context, log *zap.Logger, remote storage.Backend, remotePrefix, dbName, collectionsCSV string, excludeCollections ...string) ([]NSSpec, error) {
	dbName = strings.TrimSpace(dbName)
	if dbName == "" {
		return nil, fmt.Errorf("--db is required (mongomig.yaml or --db)")
	}
	collectionsCSV = strings.TrimSpace(collectionsCSV)
	if collectionsCSV == "" {
		names, err := DiscoverCollectionsOnOSS(ctx, log, remote, remotePrefix, dbName)
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
		log.Debug("using explicit restore collection list (must exist on OSS)", zap.String("db", dbName), zap.String("collections", collectionsCSV))
	}
	specs, err := parseCollectionsCSV(dbName, collectionsCSV)
	if err != nil {
		return nil, err
	}
	specs, err = filterExcludedSpecs(specs, log, excludeCollections...)
	if err != nil {
		return nil, err
	}
	for _, ns := range specs {
		prefix := dbOSSPrefix(remotePrefix, ns.DB)
		keys, err := remote.ListKeys(ctx, prefix)
		if err != nil {
			return nil, fmt.Errorf("list OSS keys under %q: %w", strings.TrimSuffix(prefix, "/"), err)
		}
		collBase := CollectionBase(remotePrefix, ns.DB, ns.Coll)
		if !collectionMetaOnOSS(keys, collBase) {
			return nil, fmt.Errorf("%s: no backup on OSS at %s (run mongomig backup first)", ns.String(), CollectionMetaKey(collBase))
		}
	}
	return specs, nil
}

func parseCollectionsCSV(dbName, collectionsCSV string) ([]NSSpec, error) {
	var out []NSSpec
	for _, part := range strings.Split(collectionsCSV, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if db, coll, ok := strings.Cut(part, "."); ok && strings.TrimSpace(coll) != "" {
			db, coll = strings.TrimSpace(db), strings.TrimSpace(coll)
			if db == "" {
				return nil, fmt.Errorf("invalid collection %q", part)
			}
			out = append(out, NSSpec{DB: db, Coll: coll})
			continue
		}
		out = append(out, NSSpec{DB: dbName, Coll: part})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no collections in --collections")
	}
	return out, nil
}
