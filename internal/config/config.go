package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Root holds all CLI / file configuration.
type Root struct {
	MongoURI string `json:"mongo_uri"`

	// Paths to Database Tools binaries (empty = look up in PATH).
	MongodumpPath    string `json:"mongodump_path,omitempty"`
	MongorestorePath string `json:"mongorestore_path,omitempty"`

	// Staging directory for dump output before optional upload.
	StagingDir string `json:"staging_dir"`

	// Dump behavior
	ParallelCollections int           `json:"parallel_collections,omitempty"`
	Gzip                bool          `json:"gzip,omitempty"`
	DumpTimeout         time.Duration `json:"dump_timeout,omitempty"`
	RestoreTimeout      time.Duration `json:"restore_timeout,omitempty"`

	// Optional S3-compatible OSS (nil or zero Endpoint = disabled, use local only).
	S3 *S3 `json:"s3,omitempty"`

	// Object key prefix for remote layout, e.g. "mongomig/prod".
	RemotePrefix string `json:"remote_prefix,omitempty"`

	LogLevel string `json:"log_level,omitempty"`

	// Default database for backup when using collection auto-discovery.
	DB string `json:"db,omitempty"`

	// RestoreCheckpointCollection stores restore progress (default _mongomig_restore).
	RestoreCheckpointCollection string `json:"restore_checkpoint_collection,omitempty"`
}

// DefaultRestoreCheckpointCollection is the MongoDB collection name for restore progress documents.
const DefaultRestoreCheckpointCollection = "_mongomig_restore"

type S3 struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	Bucket          string `json:"bucket"`
	AccessKeyID     string `json:"-"`
	SecretAccessKey string `json:"-"`
	UsePathStyle    bool   `json:"use_path_style,omitempty"`
}

func (r *Root) Validate() error {
	if r == nil {
		return errors.New("config is nil")
	}
	if r.MongoURI == "" {
		return errors.New("mongo_uri is required (mongomig.yaml or --mongo-uri)")
	}
	if r.StagingDir == "" {
		return errors.New("staging_dir is required")
	}
	if r.ParallelCollections < 0 {
		return errors.New("parallel_collections must be >= 0")
	}
	if r.S3 != nil && r.S3.Endpoint != "" {
		if r.S3.Bucket == "" {
			return errors.New("s3.bucket is required when s3 is enabled")
		}
		if r.S3.Region == "" {
			return errors.New("s3.region is required when s3 is enabled")
		}
	}
	return nil
}

// EnsureStaging creates staging dir if missing.
func (r *Root) EnsureStaging() error {
	if err := os.MkdirAll(r.StagingDir, 0o750); err != nil {
		return fmt.Errorf("staging_dir: %w", err)
	}
	return nil
}

// AbsStaging returns absolute staging path joined with elem.
func (r *Root) AbsStaging(elem ...string) string {
	return filepath.Join(append([]string{r.StagingDir}, elem...)...)
}

func (r *Root) S3Enabled() bool {
	return r.S3 != nil && r.S3.Endpoint != "" && r.S3.Bucket != ""
}
