package backup

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MinMaxTimeFieldForTenant returns min and max of timeField for documents matching tenantField == tenantKey.
// If no matching documents or time values are null, ok is false.
func MinMaxTimeFieldForTenant(ctx context.Context, mongoURI string, ns NSSpec, tenantField, tenantKey string, tenantNumeric bool, timeField string) (minT, maxT time.Time, ok bool, err error) {
	if err := ValidateBSONFieldName(tenantField); err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	if err := ValidateBSONFieldName(timeField); err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Minute)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	match, err := tenantMatchBSON(tenantField, tenantKey, tenantNumeric)
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	coll := client.Database(ns.DB).Collection(ns.Coll)
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "minT", Value: bson.D{{Key: "$min", Value: "$" + timeField}}},
			{Key: "maxT", Value: bson.D{{Key: "$max", Value: "$" + timeField}}},
		}}},
	}
	cur, err := coll.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("aggregate %s.%s: %w", ns.DB, ns.Coll, err)
	}
	defer func() { _ = cur.Close(ctx) }()
	if !cur.Next(ctx) {
		if err := cur.Err(); err != nil {
			return time.Time{}, time.Time{}, false, err
		}
		return time.Time{}, time.Time{}, false, nil
	}
	var row struct {
		MinT *time.Time `bson:"minT"`
		MaxT *time.Time `bson:"maxT"`
	}
	if err := cur.Decode(&row); err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	if row.MinT == nil || row.MaxT == nil {
		return time.Time{}, time.Time{}, false, nil
	}
	return *row.MinT, *row.MaxT, true, nil
}

func tenantMatchBSON(tenantField, tenantKey string, tenantNumeric bool) (bson.M, error) {
	if tenantNumeric {
		// Interpret tenantKey as JSON literal (number / bool / null) for $match.
		var v any
		if err := bson.UnmarshalExtJSON([]byte(tenantKey), true, &v); err != nil {
			return nil, fmt.Errorf("tenant-key-numeric: parse %q: %w", tenantKey, err)
		}
		return bson.M{tenantField: v}, nil
	}
	return bson.M{tenantField: tenantKey}, nil
}

// GlobalTenantTimeBounds scans each namespace and returns the earliest min and latest max across collections.
func GlobalTenantTimeBounds(ctx context.Context, mongoURI string, specs []NSSpec, tenantField, tenantKey string, tenantNumeric bool, timeField string) (minT, maxT time.Time, ok bool, err error) {
	var gmin, gmax *time.Time
	for _, ns := range specs {
		lo, hi, has, err := MinMaxTimeFieldForTenant(ctx, mongoURI, ns, tenantField, tenantKey, tenantNumeric, timeField)
		if err != nil {
			return time.Time{}, time.Time{}, false, fmt.Errorf("%s.%s: %w", ns.DB, ns.Coll, err)
		}
		if !has {
			continue
		}
		if gmin == nil || lo.Before(*gmin) {
			t := lo
			gmin = &t
		}
		if gmax == nil || hi.After(*gmax) {
			t := hi
			gmax = &t
		}
	}
	if gmin == nil || gmax == nil {
		return time.Time{}, time.Time{}, false, nil
	}
	return *gmin, *gmax, true, nil
}
