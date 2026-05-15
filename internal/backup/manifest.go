package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Type is full or incremental backup artifact.
type Type string

const (
	TypeFull           Type = "full"
	TypeIncr           Type = "incr"
	TypeTenantSnapshot Type = "tenant_snapshot"
)

// Manifest is persisted after each successful backup.
type Manifest struct {
	BackupID    string     `json:"backup_id"`
	CreatedAt   time.Time  `json:"created_at"`
	Type        Type       `json:"type"`
	LastOplogTS *OplogTS   `json:"last_oplog_ts,omitempty"`
	MongoTools  string     `json:"mongo_tools_hint,omitempty"`
	Notes       string     `json:"notes,omitempty"`
	// Tenant OSS snapshot (TypeTenantSnapshot)
	TenantKey            string `json:"tenant_key,omitempty"`
	PartitionCreatedAt   string `json:"partition_created_at,omitempty"`   // RFC3339Nano UTC
	PartitionPathSegment string `json:"partition_path_segment,omitempty"` // OSS path segment for created_at
	BackupDate           string `json:"backup_date,omitempty"`            // YYYY-MM-DD UTC (compat / convenience)
	TenantKeyField       string `json:"tenant_key_field,omitempty"`
	Collections          []string `json:"collections,omitempty"` // "db.collection"
	// Day-scoped tenant export (optional; see oss export-days)
	TimeField         string `json:"time_field,omitempty"`
	TimeRangeStartUTC string `json:"time_range_start_utc,omitempty"`
	TimeRangeEndUTC   string `json:"time_range_end_utc,omitempty"` // exclusive end
	ExportCivilDay    string `json:"export_civil_day,omitempty"`   // YYYY-MM-DD in export_timezone
	ExportTimezone    string `json:"export_timezone,omitempty"`
}

// OplogTS is BSON Timestamp as JSON-friendly pair.
type OplogTS struct {
	T uint32 `json:"t"`
	I uint32 `json:"i"`
}

func (o *OplogTS) ToPrimitive() primitive.Timestamp {
	if o == nil {
		return primitive.Timestamp{}
	}
	return primitive.Timestamp{T: o.T, I: o.I}
}

func OplogTSFromPrimitive(ts primitive.Timestamp) *OplogTS {
	return &OplogTS{T: ts.T, I: ts.I}
}

func WriteManifest(path string, m *Manifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	tmp := path + ".tmp"
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func ReadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	return &m, nil
}
