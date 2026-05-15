package backup

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CheckOplogWindow returns nil if oldest oplog entry is strictly before or equal to last,
// meaning last is still reachable for incremental chain (last must still exist in oplog).
// If last is zero, always ok.
func CheckOplogWindow(ctx context.Context, uri string, last primitive.Timestamp) error {
	if last.T == 0 && last.I == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database("local").Collection("oplog.rs")
	var oldest struct {
		TS primitive.Timestamp `bson:"ts"`
	}
	err = coll.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: 1}})).Decode(&oldest)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return fmt.Errorf("oplog.rs is empty")
		}
		return fmt.Errorf("read oldest oplog: %w", err)
	}
	// If oldest is after last, we've lost continuity.
	if tsAfter(oldest.TS, last) {
		return fmt.Errorf("oplog window lost: oldest ts %v/%v is after last backup ts %v/%v; take a new full backup",
			oldest.TS.T, oldest.TS.I, last.T, last.I)
	}
	return nil
}

// LatestOplogTS returns the maximum ts currently in oplog.rs (approximate upper bound for dump).
func LatestOplogTS(ctx context.Context, uri string) (primitive.Timestamp, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return primitive.Timestamp{}, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database("local").Collection("oplog.rs")
	var newest struct {
		TS primitive.Timestamp `bson:"ts"`
	}
	err = coll.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})).Decode(&newest)
	if err != nil {
		return primitive.Timestamp{}, err
	}
	return newest.TS, nil
}

// WriteOplogQueryFile writes Extended JSON query for ts > after.
func WriteOplogQueryFile(path string, after primitive.Timestamp) error {
	// Extended JSON for Timestamp: {"$timestamp":{"t":..,"i":..}}
	q := fmt.Sprintf(`{"ts":{"$gt":{"$timestamp":{"t":%d,"i":%d}}}}`, after.T, after.I)
	return os.WriteFile(path, []byte(q), 0o600)
}
