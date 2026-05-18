package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/logeable/mongomig/internal/storage"
)

const hourlyMetaSchema = 1

type HourStatus string

const (
	HourStatusPartial  HourStatus = "partial"
	HourStatusComplete HourStatus = "complete"
)

// HourRef summarizes a UTC hour partition in collection meta.
type HourRef struct {
	Year              int        `json:"year"`
	Month             int        `json:"month"`
	Day               int        `json:"day"`
	Hour              int        `json:"hour"`
	IntervalStartUTC  string     `json:"interval_start_utc"`
	IntervalEndUTC    string     `json:"interval_end_utc"`
	HourMetaRef       string     `json:"hour_meta_ref"`
	Status            HourStatus `json:"status,omitempty"`
}

func HourRefFromBucket(b HourBucket, hourMetaRef string, status HourStatus) HourRef {
	return HourRef{
		Year:             b.Year,
		Month:            b.Month,
		Day:              b.Day,
		Hour:             b.Hour,
		IntervalStartUTC: b.Start.UTC().Format(time.RFC3339Nano),
		IntervalEndUTC:   b.End.UTC().Format(time.RFC3339Nano),
		HourMetaRef:      hourMetaRef,
		Status:           status,
	}
}

func (r HourRef) Bucket() HourBucket {
	return HourBucket{
		Year:  r.Year,
		Month: r.Month,
		Day:   r.Day,
		Hour:  r.Hour,
		Start: mustParseRFC3339(r.IntervalStartUTC),
		End:   mustParseRFC3339(r.IntervalEndUTC),
	}
}

func mustParseRFC3339(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, _ = time.Parse(time.RFC3339, s)
	}
	return t.UTC()
}

// CollectionMeta is stored at {prefix}/{db}/{collection}/meta.json.
type CollectionMeta struct {
	Schema           int        `json:"schema"`
	DB               string     `json:"db"`
	Collection       string     `json:"collection"`
	OldestCompleted  *HourRef   `json:"oldest_completed,omitempty"`
	NewestCompleted  *HourRef   `json:"newest_completed,omitempty"`
	Active           *HourRef   `json:"active,omitempty"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// TenantMetaRow is one tenant in an hour backup.
// DataRelPath is relative to the hour prefix ({year}/{month}/{day}/{hour}/).
type TenantMetaRow struct {
	TenantKey   string `json:"tenant_key"`
	Shard       string `json:"shard"`
	DataRelPath string `json:"data_rel_path"`
	Uploaded    bool   `json:"uploaded"`
	SHA256      string `json:"sha256,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	Error       string `json:"error,omitempty"`
}

// HourMeta is stored at {hour}/meta.json.
type HourMeta struct {
	Schema            int             `json:"schema"`
	Status            HourStatus      `json:"status"`
	IntervalStartUTC  string          `json:"interval_start_utc"`
	IntervalEndUTC    string          `json:"interval_end_utc"`
	Year              int             `json:"year"`
	Month             int             `json:"month"`
	Day               int             `json:"day"`
	Hour              int             `json:"hour"`
	DB                string          `json:"db"`
	Collection        string          `json:"collection"`
	Tenants           []TenantMetaRow `json:"tenants"`
	CompletedAt       string          `json:"completed_at,omitempty"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

type MetaStore struct {
	remote storage.Backend
	tmpDir string
}

func NewMetaStore(remote storage.Backend, stagingDir string) (*MetaStore, error) {
	tmp := filepath.Join(stagingDir, "_meta_upload")
	if err := os.MkdirAll(tmp, 0o750); err != nil {
		return nil, err
	}
	return &MetaStore{remote: remote, tmpDir: tmp}, nil
}

func (s *MetaStore) loadJSON(ctx context.Context, key string, dest any) (bool, error) {
	local := filepath.Join(s.tmpDir, "load-"+filepath.Base(key))
	if err := s.remote.DownloadFile(ctx, key, local); err != nil {
		if isRemoteMissing(err) {
			return false, nil
		}
		return false, err
	}
	data, err := os.ReadFile(local)
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return false, fmt.Errorf("parse %s: %w", key, err)
	}
	return true, nil
}

func (s *MetaStore) saveJSON(ctx context.Context, key string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	local := filepath.Join(s.tmpDir, "save-"+filepath.Base(key))
	if err := os.WriteFile(local, data, 0o640); err != nil {
		return err
	}
	return s.remote.UploadFile(ctx, local, key)
}

func (s *MetaStore) LoadCollectionMeta(ctx context.Context, collectionBase string) (*CollectionMeta, bool, error) {
	key := CollectionMetaKey(collectionBase)
	var m CollectionMeta
	ok, err := s.loadJSON(ctx, key, &m)
	if err != nil || !ok {
		return nil, ok, err
	}
	return &m, true, nil
}

func (s *MetaStore) SaveCollectionMeta(ctx context.Context, collectionBase string, m *CollectionMeta) error {
	m.Schema = hourlyMetaSchema
	m.UpdatedAt = time.Now().UTC()
	return s.saveJSON(ctx, CollectionMetaKey(collectionBase), m)
}

func (s *MetaStore) LoadHourMeta(ctx context.Context, hourBase string) (*HourMeta, bool, error) {
	key := HourMetaKey(hourBase)
	var m HourMeta
	ok, err := s.loadJSON(ctx, key, &m)
	if err != nil || !ok {
		return nil, ok, err
	}
	return &m, true, nil
}

func (s *MetaStore) SaveHourMeta(ctx context.Context, hourBase string, m *HourMeta) error {
	m.Schema = hourlyMetaSchema
	m.UpdatedAt = time.Now().UTC()
	return s.saveJSON(ctx, HourMetaKey(hourBase), m)
}

func HourMetaRefRelative(b HourBucket) string {
	return b.PathSegment() + "/meta.json"
}
