package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/logeable/mongomig/internal/config"
)

// NSSpec is a database.collection pair for tenant-filtered dumps.
type NSSpec struct {
	DB   string
	Coll string
}

// SanitizeTenantPath makes a tenant id safe as a single OSS/FS path segment.
func SanitizeTenantPath(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("tenant_key is empty")
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '/' || r == '\\':
			b.WriteByte('_')
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := b.String()
	if out == "" || out == "." || out == ".." {
		return "", fmt.Errorf("tenant_key sanitizes to invalid segment %q", out)
	}
	return out, nil
}

// ParseCollectionSpecs parses comma-separated "db.collection" entries (only first dot splits DB from coll).
func ParseCollectionSpecs(csv string) ([]NSSpec, error) {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return nil, fmt.Errorf("collections is empty")
	}
	var out []NSSpec
	for _, part := range strings.Split(csv, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		db, coll, ok := strings.Cut(part, ".")
		db, coll = strings.TrimSpace(db), strings.TrimSpace(coll)
		if !ok || db == "" || coll == "" {
			return nil, fmt.Errorf("invalid collection %q (want db.collection)", part)
		}
		out = append(out, NSSpec{DB: db, Coll: coll})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no collections parsed")
	}
	return out, nil
}

// WriteTenantQueryFile writes a JSON query filter for tenant_key field.
func WriteTenantQueryFile(path, field, value string, numeric bool) error {
	if numeric {
		raw := fmt.Sprintf(`{"%s": %s}`, field, value)
		return os.WriteFile(path, []byte(raw), 0o600)
	}
	m := map[string]any{field: value}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// RunTenantSnapshot dumps listed collections with {field: tenantKey}, writes index catalog JSON,
// and uploads to OSS path: {remotePrefix}/{tenantSegment}/{partitionPath}/{backup_id}/...
//
// Data + index fidelity for restore uses MongoDB Database Tools: each collection produces
// *.bson(.gz) and *.metadata.json(.gz) which mongorestore applies to recreate indexes.
// _indexes/*.json is an additional human-readable copy of listIndexes output.
func (r *Runner) RunTenantSnapshot(ctx context.Context, tenantKey string, partition time.Time, specs []NSSpec, tenantField string, numericKey, cleanupLocal bool) (*Manifest, error) {
	if !r.cfg.S3Enabled() {
		return nil, fmt.Errorf("OSS export requires S3 (--s3-endpoint and bucket)")
	}
	if err := r.cfg.EnsureStaging(); err != nil {
		return nil, err
	}
	tenantSeg, err := SanitizeTenantPath(tenantKey)
	if err != nil {
		return nil, err
	}
	pathSeg := PartitionPathSegment(partition)
	field := strings.TrimSpace(tenantField)
	if field == "" {
		field = "tenant_key"
	}

	id := time.Now().UTC().Format("20060102T150405.000000000Z")
	localRoot := filepath.Join(r.cfg.StagingDir, "tenants", tenantSeg, pathSeg, id)
	if err := os.MkdirAll(localRoot, 0o750); err != nil {
		return nil, err
	}

	for _, ns := range specs {
		qf := filepath.Join(localRoot, queryFileName(ns))
		if err := WriteTenantQueryFile(qf, field, tenantKey, numericKey); err != nil {
			return nil, err
		}
		if err := r.tools.DumpCollectionQuery(ctx, ns.DB, ns.Coll, localRoot, qf); err != nil {
			return nil, fmt.Errorf("dump %s.%s: %w", ns.DB, ns.Coll, err)
		}
		_ = os.Remove(qf)
	}

	if err := WriteIndexCatalog(ctx, r.cfg.MongoURI, localRoot, specs); err != nil {
		return nil, fmt.Errorf("index catalog: %w", err)
	}

	cols := make([]string, 0, len(specs))
	for _, ns := range specs {
		cols = append(cols, ns.DB+"."+ns.Coll)
	}
	m := &Manifest{
		BackupID:             id,
		CreatedAt:            time.Now().UTC(),
		Type:                 TypeTenantSnapshot,
		TenantKey:            tenantKey,
		PartitionCreatedAt:   partition.UTC().Format(time.RFC3339Nano),
		PartitionPathSegment: pathSeg,
		BackupDate:           partition.UTC().Format("2006-01-02"),
		TenantKeyField:       field,
		Collections:          cols,
	}
	if err := WriteManifest(filepath.Join(localRoot, "manifest.json"), m); err != nil {
		return nil, err
	}

	remoteBase := filepath.ToSlash(filepath.Join(r.cfg.RemotePrefix, tenantSeg, pathSeg, id))
	if err := walkUpload(ctx, r.remote, localRoot, remoteBase); err != nil {
		return nil, err
	}
	if cleanupLocal {
		_ = os.RemoveAll(localRoot)
	}
	return m, nil
}

func queryFileName(ns NSSpec) string {
	safeDB := strings.Map(safePathRune, ns.DB)
	safeColl := strings.Map(safePathRune, ns.Coll)
	return fmt.Sprintf("_query_%s_%s.json", safeDB, safeColl)
}

func safePathRune(r rune) rune {
	if r == '/' || r == '\\' || r == '.' {
		return '_'
	}
	if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' {
		return r
	}
	return '_'
}

// TenantOSSPrefix returns the remote prefix for a tenant + partition path (all runs under that partition).
func TenantOSSPrefix(cfg *config.Root, tenantSeg, partitionPathSegment string) string {
	return filepath.ToSlash(filepath.Join(cfg.RemotePrefix, tenantSeg, partitionPathSegment))
}
