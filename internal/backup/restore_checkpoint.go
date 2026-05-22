package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/logeable/mongomig/internal/config"
)

const restoreCheckpointSchema = 2

// RestoreCheckpoint is persisted in MongoDB (see config.DefaultRestoreCheckpointCollection).
// newest_restored advances when an entire UTC hour finishes on OSS; active is audit-only.
type RestoreCheckpoint struct {
	ID             string    `bson:"_id"`
	Schema         int       `bson:"schema"`
	DB             string    `bson:"db"`
	Collection     string    `bson:"collection"`
	MongoURIHash   string    `bson:"mongo_uri_hash"`
	RemotePrefix   string    `bson:"remote_prefix"`
	CollectionBase string    `bson:"collection_base"`
	OldestRestored *HourRef  `bson:"oldest_restored,omitempty"`
	NewestRestored *HourRef  `bson:"newest_restored,omitempty"`
	// Active records the last UTC hour whose uploaded tenants were restored (OSS status at restore time).
	// Not used for scheduling or skip logic.
	Active    *HourRef  `bson:"active,omitempty"`
	UpdatedAt time.Time `bson:"updated_at"`
}

// RestoreCheckpointStore reads/writes checkpoints in a dedicated MongoDB collection.
type RestoreCheckpointStore struct {
	client   *mongo.Client
	collName string
}

func NewRestoreCheckpointStore(ctx context.Context, mongoURI, checkpointCollection string) (*RestoreCheckpointStore, error) {
	collName := strings.TrimSpace(checkpointCollection)
	if collName == "" {
		collName = config.DefaultRestoreCheckpointCollection
	}
	if err := ValidateBSONFieldName(collName); err != nil {
		return nil, fmt.Errorf("checkpoint collection: %w", err)
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, fmt.Errorf("connect for restore checkpoint: %w", err)
	}
	return &RestoreCheckpointStore{client: client, collName: collName}, nil
}

func (s *RestoreCheckpointStore) Disconnect(ctx context.Context) {
	if s != nil && s.client != nil {
		_ = s.client.Disconnect(ctx)
	}
}

func (s *RestoreCheckpointStore) checkpointColl(db string) *mongo.Collection {
	return s.client.Database(db).Collection(s.collName)
}

func restoreCheckpointID(remotePrefix, db, coll string) string {
	return strings.Trim(remotePrefix, "/") + "/" + db + "/" + coll
}

func mongoURIHash(mongoURI string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(mongoURI)))
	return hex.EncodeToString(sum[:8])
}

// Load returns a checkpoint or nil if missing. Validates target identity when a document exists.
func (s *RestoreCheckpointStore) Load(ctx context.Context, remotePrefix, collectionBase, mongoURI string, ns NSSpec) (*RestoreCheckpoint, error) {
	id := restoreCheckpointID(remotePrefix, ns.DB, ns.Coll)
	var cp RestoreCheckpoint
	err := s.checkpointColl(ns.DB).FindOne(ctx, bson.M{"_id": id}).Decode(&cp)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load restore checkpoint %s.%s.%s: %w", ns.DB, s.collName, id, err)
	}
	wantHash := mongoURIHash(mongoURI)
	if cp.MongoURIHash != wantHash {
		return nil, fmt.Errorf("checkpoint %s mongo_uri_hash mismatch (another cluster? use --reset-checkpoint)", id)
	}
	if cp.CollectionBase != collectionBase {
		return nil, fmt.Errorf("checkpoint %s collection_base mismatch (use --reset-checkpoint)", id)
	}
	if cp.RemotePrefix != strings.Trim(remotePrefix, "/") {
		return nil, fmt.Errorf("checkpoint %s remote_prefix mismatch (use --reset-checkpoint)", id)
	}
	if cp.DB != ns.DB || cp.Collection != ns.Coll {
		return nil, fmt.Errorf("checkpoint %s db/collection mismatch", id)
	}
	return &cp, nil
}

func (s *RestoreCheckpointStore) Save(ctx context.Context, cp *RestoreCheckpoint, ns NSSpec) error {
	if cp == nil {
		return nil
	}
	cp.Schema = restoreCheckpointSchema
	cp.UpdatedAt = time.Now().UTC()
	if cp.ID == "" {
		cp.ID = restoreCheckpointID(cp.RemotePrefix, ns.DB, ns.Coll)
	}
	_, err := s.checkpointColl(ns.DB).ReplaceOne(ctx, bson.M{"_id": cp.ID}, cp, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("save restore checkpoint %s: %w", cp.ID, err)
	}
	return nil
}

func (s *RestoreCheckpointStore) Remove(ctx context.Context, remotePrefix string, ns NSSpec) error {
	id := restoreCheckpointID(remotePrefix, ns.DB, ns.Coll)
	_, err := s.checkpointColl(ns.DB).DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return fmt.Errorf("delete restore checkpoint %s: %w", id, err)
	}
	return nil
}

func newRestoreCheckpoint(remotePrefix, collectionBase, mongoURI string, ns NSSpec) *RestoreCheckpoint {
	return &RestoreCheckpoint{
		ID:             restoreCheckpointID(remotePrefix, ns.DB, ns.Coll),
		Schema:         restoreCheckpointSchema,
		DB:             ns.DB,
		Collection:     ns.Coll,
		MongoURIHash:   mongoURIHash(mongoURI),
		RemotePrefix:   strings.Trim(remotePrefix, "/"),
		CollectionBase: collectionBase,
	}
}

// hourAlreadyRestored reports whether hb is at or before the newest fully restored hour.
func (cp *RestoreCheckpoint) hourAlreadyRestored(hb HourBucket) bool {
	if cp == nil || cp.NewestRestored == nil {
		return false
	}
	return !hb.Start.After(cp.NewestRestored.Bucket().Start)
}

func (cp *RestoreCheckpoint) setRestoreActive(hb HourBucket, ossStatus HourStatus) {
	ref := HourRefFromBucket(hb, HourMetaRefRelative(hb), ossStatus)
	cp.Active = &ref
}

func (cp *RestoreCheckpoint) markHourComplete(hb HourBucket) {
	ref := HourRefFromBucket(hb, HourMetaRefRelative(hb), HourStatusComplete)
	cp.NewestRestored = &ref
	cp.Active = &ref
	if cp.OldestRestored == nil {
		cp.OldestRestored = &ref
	}
}
