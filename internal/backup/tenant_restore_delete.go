package backup

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// DeleteTenantHourSlice removes documents matching the same filter as backup/mongodump for one tenant hour.
// Used before mongorestore so re-running a hour overwrites that slice instead of duplicate _id errors.
func DeleteTenantHourSlice(ctx context.Context, mongoURI string, log *zap.Logger, ns NSSpec, tenantField, tenantKey string, tenantNumeric bool, timeField string, hb HourBucket) (int64, error) {
	filter, err := TenantHourRangeFilter(tenantField, tenantKey, tenantNumeric, timeField, hb.Start, hb.End)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return 0, fmt.Errorf("connect for delete: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	res, err := client.Database(ns.DB).Collection(ns.Coll).DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("delete %s tenant %s hour %s: %w", ns.String(), tenantKey, hb.String(), err)
	}
	if log != nil && res.DeletedCount > 0 {
		log.Debug("deleted tenant hour slice before restore",
			zap.String("collection", ns.String()),
			zap.String("hour", hb.String()),
			zap.String("tenant", tenantKey),
			zap.Int64("deleted", res.DeletedCount),
		)
	}
	return res.DeletedCount, nil
}
