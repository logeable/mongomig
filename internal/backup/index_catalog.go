package backup

import (
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// indexKeyPair is one field in an index key (JSON array order = index key order).
type indexKeyPair struct {
	Field string          `json:"field"`
	Value json.RawMessage `json:"value"`
}

// indexCatalogEntry is the JSON shape of one index in indexes.json.
type indexCatalogEntry struct {
	Name                    string          `json:"name,omitempty"`
	Key                     []indexKeyPair `json:"key"`
	Unique                  *bool           `json:"unique,omitempty"`
	Sparse                  *bool           `json:"sparse,omitempty"`
	Hidden                  *bool           `json:"hidden,omitempty"`
	ExpireAfterSeconds      *int32          `json:"expireAfterSeconds,omitempty"`
	PartialFilterExpression json.RawMessage `json:"partialFilterExpression,omitempty"`
	Collation               json.RawMessage `json:"collation,omitempty"`
	Weights                 json.RawMessage `json:"weights,omitempty"`
	DefaultLanguage         string          `json:"default_language,omitempty"`
	LanguageOverride        string          `json:"language_override,omitempty"`
	TextIndexVersion        *int32          `json:"textIndexVersion,omitempty"`
}

type collectionIndexesFile struct {
	DB         string              `json:"db"`
	Collection string              `json:"collection"`
	Indexes    []indexCatalogEntry `json:"indexes"`
	UpdatedAt  string              `json:"updated_at"`
}

func indexKeyPairsFromD(d bson.D) ([]indexKeyPair, error) {
	if len(d) == 0 {
		return nil, fmt.Errorf("index key is empty")
	}
	pairs := make([]indexKeyPair, 0, len(d))
	for _, elem := range d {
		valJSON, err := marshalIndexKeyValue(elem.Value)
		if err != nil {
			return nil, fmt.Errorf("index key field %q: %w", elem.Key, err)
		}
		pairs = append(pairs, indexKeyPair{Field: elem.Key, Value: valJSON})
	}
	return pairs, nil
}

func bsonDFromIndexKeyPairs(pairs []indexKeyPair) (bson.D, error) {
	if len(pairs) == 0 {
		return nil, fmt.Errorf("index key is empty")
	}
	d := make(bson.D, 0, len(pairs))
	for _, p := range pairs {
		v, err := unmarshalIndexKeyValue(p.Value)
		if err != nil {
			return nil, fmt.Errorf("index key field %q: %w", p.Field, err)
		}
		d = append(d, bson.E{Key: p.Field, Value: v})
	}
	return d, nil
}

func indexCatalogEntryFromBSONRaw(raw bson.Raw) (indexCatalogEntry, error) {
	var spec struct {
		Key  bson.D `bson:"key"`
		Name string `bson:"name"`
	}
	if err := bson.Unmarshal(raw, &spec); err != nil {
		return indexCatalogEntry{}, err
	}
	pairs, err := indexKeyPairsFromD(spec.Key)
	if err != nil {
		return indexCatalogEntry{}, err
	}
	entry := indexCatalogEntry{Name: spec.Name, Key: pairs}
	var full bson.M
	if err := bson.Unmarshal(raw, &full); err != nil {
		return entry, nil
	}
	if v, ok := full["unique"].(bool); ok {
		entry.Unique = &v
	}
	if v, ok := full["sparse"].(bool); ok {
		entry.Sparse = &v
	}
	if v, ok := full["hidden"].(bool); ok {
		entry.Hidden = &v
	}
	if v, ok := full["expireAfterSeconds"]; ok {
		switch n := v.(type) {
		case int32:
			entry.ExpireAfterSeconds = &n
		case int64:
			x := int32(n)
			entry.ExpireAfterSeconds = &x
		case float64:
			x := int32(n)
			entry.ExpireAfterSeconds = &x
		}
	}
	if v, ok := full["partialFilterExpression"]; ok && v != nil {
		entry.PartialFilterExpression, _ = marshalExtJSONValue(v)
	}
	if v, ok := full["collation"]; ok && v != nil {
		entry.Collation, _ = marshalExtJSONValue(v)
	}
	if v, ok := full["weights"]; ok && v != nil {
		entry.Weights, _ = marshalExtJSONValue(v)
	}
	if v, ok := full["default_language"].(string); ok {
		entry.DefaultLanguage = v
	}
	if v, ok := full["language_override"].(string); ok {
		entry.LanguageOverride = v
	}
	if v, ok := full["textIndexVersion"]; ok {
		switch n := v.(type) {
		case int32:
			entry.TextIndexVersion = &n
		case int64:
			x := int32(n)
			entry.TextIndexVersion = &x
		}
	}
	return entry, nil
}

func marshalIndexKeyValue(v any) (json.RawMessage, error) {
	switch v.(type) {
	case int32, int64, int, float32, float64, string, bool:
		return json.Marshal(v)
	default:
		data, err := bson.MarshalExtJSON(v, true, false)
		if err != nil {
			return nil, err
		}
		return json.RawMessage(data), nil
	}
}

func unmarshalIndexKeyValue(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty value")
	}
	if raw[0] == '{' || raw[0] == '[' {
		var v any
		if err := bson.UnmarshalExtJSON(raw, true, &v); err != nil {
			return nil, err
		}
		return v, nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	if f, ok := v.(float64); ok && f == float64(int64(f)) {
		return int(f), nil
	}
	return v, nil
}

func marshalExtJSONValue(v any) (json.RawMessage, error) {
	data, err := bson.MarshalExtJSON(v, true, false)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func indexModelFromCatalogEntry(entry indexCatalogEntry) (mongo.IndexModel, bool, error) {
	if entry.Name == "_id_" {
		return mongo.IndexModel{}, false, nil
	}
	key, err := bsonDFromIndexKeyPairs(entry.Key)
	if err != nil {
		return mongo.IndexModel{}, false, fmt.Errorf("index %q: %w", entry.Name, err)
	}
	opts := options.Index()
	if entry.Name != "" {
		opts.SetName(entry.Name)
	}
	if entry.Unique != nil && *entry.Unique {
		opts.SetUnique(true)
	}
	if entry.Sparse != nil && *entry.Sparse {
		opts.SetSparse(true)
	}
	if entry.Hidden != nil && *entry.Hidden {
		opts.SetHidden(true)
	}
	if entry.ExpireAfterSeconds != nil {
		opts.SetExpireAfterSeconds(*entry.ExpireAfterSeconds)
	}
	if len(entry.PartialFilterExpression) > 0 {
		var expr bson.M
		if err := bson.UnmarshalExtJSON(entry.PartialFilterExpression, true, &expr); err != nil {
			return mongo.IndexModel{}, false, err
		}
		opts.SetPartialFilterExpression(expr)
	}
	if len(entry.Collation) > 0 {
		var c options.Collation
		if err := bson.UnmarshalExtJSON(entry.Collation, true, &c); err != nil {
			return mongo.IndexModel{}, false, err
		}
		opts.SetCollation(&c)
	}
	if len(entry.Weights) > 0 {
		var w bson.M
		if err := bson.UnmarshalExtJSON(entry.Weights, true, &w); err != nil {
			return mongo.IndexModel{}, false, err
		}
		opts.SetWeights(w)
	}
	if entry.DefaultLanguage != "" {
		opts.SetDefaultLanguage(entry.DefaultLanguage)
	}
	if entry.LanguageOverride != "" {
		opts.SetLanguageOverride(entry.LanguageOverride)
	}
	if entry.TextIndexVersion != nil {
		opts.SetTextVersion(*entry.TextIndexVersion)
	}
	return mongo.IndexModel{Keys: key, Options: opts}, true, nil
}
