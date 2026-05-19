package backup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MinTimeFieldForCollection returns the earliest timeField in the collection using an
// index-friendly find (sort ascending, limit 1). Requires an index on timeField for
// large collections; use MinMaxTimeFieldForCollection when both bounds are needed without an index.
func MinTimeFieldForCollection(ctx context.Context, mongoURI string, ns NSSpec, timeField string) (minT time.Time, ok bool, err error) {
	if err := ValidateBSONFieldName(timeField); err != nil {
		return time.Time{}, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return time.Time{}, false, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database(ns.DB).Collection(ns.Coll)
	filter := bson.M{timeField: bson.M{"$exists": true, "$ne": nil}}
	findOpts := options.FindOne().
		SetSort(bson.D{{Key: timeField, Value: 1}}).
		SetHint(bson.D{{Key: timeField, Value: 1}}).
		SetProjection(bson.M{"_id": 0, timeField: 1})

	var doc bson.M
	err = coll.FindOne(ctx, filter, findOpts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("find min %s.%s.%s: %w", ns.DB, ns.Coll, timeField, err)
	}
	t, ok := bsonTimeValue(doc[timeField])
	if !ok {
		return time.Time{}, false, nil
	}
	return t, true, nil
}

func bsonTimeValue(v any) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		return t, true
	case *time.Time:
		if t == nil {
			return time.Time{}, false
		}
		return *t, true
	default:
		return time.Time{}, false
	}
}

// MinMaxTimeFieldForCollection returns min/max of timeField across the collection.
func MinMaxTimeFieldForCollection(ctx context.Context, mongoURI string, ns NSSpec, timeField string) (minT, maxT time.Time, ok bool, err error) {
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

	coll := client.Database(ns.DB).Collection(ns.Coll)
	pipeline := mongo.Pipeline{
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
