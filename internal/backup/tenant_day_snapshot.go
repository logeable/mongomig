package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/logeable/mongomig/internal/config"
)

// RunTenantDaySnapshot dumps collections with tenant + [startUTC,endUTC) on timeField, uploads to OSS.
// partition is used for OSS path (typically civil midnight in export TZ); civilDay and exportTZ are recorded on the manifest.
func (r *Runner) RunTenantDaySnapshot(ctx context.Context, tenantKey string, partition time.Time, civilDay, exportTZ string, specs []NSSpec, tenantField string, tenantNumeric bool, timeField string, startUTC, endUTC time.Time, cleanupLocal bool) (*Manifest, error) {
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
	tf := strings.TrimSpace(timeField)
	if tf == "" {
		tf = "created_at"
	}
	if err := ValidateBSONFieldName(tf); err != nil {
		return nil, err
	}

	id := time.Now().UTC().Format("20060102T150405.000000000Z")
	localRoot := filepath.Join(r.cfg.StagingDir, "tenants", tenantSeg, pathSeg, id)
	if err := os.MkdirAll(localRoot, 0o750); err != nil {
		return nil, err
	}

	for _, ns := range specs {
		qf := filepath.Join(localRoot, queryFileName(ns))
		if err := WriteTenantDayRangeQueryFile(qf, field, tenantKey, tenantNumeric, tf, startUTC, endUTC); err != nil {
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
		BackupDate:           civilDay,
		TenantKeyField:       field,
		Collections:          cols,
		TimeField:            tf,
		TimeRangeStartUTC:    startUTC.UTC().Format(time.RFC3339Nano),
		TimeRangeEndUTC:      endUTC.UTC().Format(time.RFC3339Nano),
		ExportCivilDay:       civilDay,
		ExportTimezone:       exportTZ,
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

// TenantDaySyncProgressKey returns the OSS object key for day-sync job state.
func TenantDaySyncProgressKey(cfg *config.Root, tenantSeg, syncJobID string) (string, error) {
	job, err := sanitizeSyncJobID(syncJobID)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(filepath.Join(cfg.RemotePrefix, tenantSeg, "_meta", fmt.Sprintf("day-sync-%s.json", job))), nil
}

func sanitizeSyncJobID(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("sync job id is empty")
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
		return "", fmt.Errorf("sync job id sanitizes to invalid %q", out)
	}
	return out, nil
}
