package backup

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DiscoverTenantsInHour returns distinct tenantField values for documents with
// timeField in [startUTC, endUTC).
func DiscoverTenantsInHour(ctx context.Context, mongoURI string, ns NSSpec, tenantField, timeField string, tenantNumeric bool, startUTC, endUTC time.Time) ([]string, error) {
	if err := ValidateBSONFieldName(tenantField); err != nil {
		return nil, err
	}
	if err := ValidateBSONFieldName(timeField); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	match := bson.M{
		timeField: bson.M{
			"$gte": startUTC.UTC(),
			"$lt":  endUTC.UTC(),
		},
	}

	coll := client.Database(ns.DB).Collection(ns.Coll)
	vals, err := coll.Distinct(ctx, tenantField, match)
	if err != nil {
		return nil, fmt.Errorf("distinct %s.%s: %w", ns.DB, ns.Coll, err)
	}
	var out []string
	for _, v := range vals {
		if v == nil {
			continue
		}
		key, err := tenantKeyString(v, tenantNumeric)
		if err != nil {
			return nil, err
		}
		if key != "" {
			out = append(out, key)
		}
	}
	return out, nil
}

func tenantKeyString(v any, numeric bool) (string, error) {
	if numeric {
		b, err := bson.MarshalExtJSON(v, false, false)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	switch t := v.(type) {
	case string:
		return t, nil
	default:
		b, err := bson.MarshalExtJSON(v, false, false)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}
