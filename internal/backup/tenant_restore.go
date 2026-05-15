package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/logeable/mongomig/internal/config"
)

// DownloadRemotePrefix downloads all objects under remotePrefix (no trailing slash required) into localRoot preserving relative paths.
func (r *RestoreRunner) DownloadRemotePrefix(ctx context.Context, remotePrefix, localRoot string) error {
	return r.downloadPrefix(ctx, remotePrefix, localRoot)
}

// ListTenantRunIDs lists backup run ids under OSS prefix {remote}/{tenant}/{partition}/.
func ListTenantRunIDs(ctx context.Context, remote storageListKeys, cfg *config.Root, tenantSeg, partitionPathSegment string) ([]string, error) {
	base := TenantOSSPrefix(cfg, tenantSeg, partitionPathSegment)
	keys, err := remote.ListKeys(ctx, base+"/")
	if err != nil {
		return nil, err
	}
	prefix := base + "/"
	seen := map[string]struct{}{}
	for _, k := range keys {
		rel, ok := cutPrefix(k, prefix)
		if !ok || rel == "" {
			continue
		}
		runID, _, ok := strings.Cut(rel, "/")
		if !ok {
			runID = rel
		}
		if runID != "" {
			seen[runID] = struct{}{}
		}
	}
	var runs []string
	for id := range seen {
		runs = append(runs, id)
	}
	sort.Strings(runs)
	return runs, nil
}

// storageListKeys matches storage.Backend for tenant_restore without import cycle — use small interface.
type storageListKeys interface {
	ListKeys(ctx context.Context, prefix string) ([]string, error)
	DownloadFile(ctx context.Context, key, localPath string) error
}

// RestoreTenant restores one tenant snapshot from OSS (requires S3).
// partitionCreatedAtInput is the same textual form as export (--created-at): YYYY-MM-DD or RFC3339.
// If runID is empty, picks the latest lexicographic run id under that partition.
func (r *RestoreRunner) RestoreTenant(ctx context.Context, tenantKey, partitionCreatedAtInput, runID string, drop bool) error {
	if !r.cfg.S3Enabled() {
		return fmt.Errorf("OSS import requires S3 to list/download objects")
	}
	if err := r.cfg.EnsureStaging(); err != nil {
		return err
	}
	tenantSeg, err := SanitizeTenantPath(tenantKey)
	if err != nil {
		return err
	}
	part, err := ParsePartitionCreatedAt(partitionCreatedAtInput)
	if err != nil {
		return err
	}
	pathSeg := PartitionPathSegment(part)

	segUsed := pathSeg
	runs, err := ListTenantRunIDs(ctx, r.remote, r.cfg, tenantSeg, segUsed)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		if flat, ok := legacyFlatDateInput(partitionCreatedAtInput); ok {
			runs, err = ListTenantRunIDs(ctx, r.remote, r.cfg, tenantSeg, flat)
			if err != nil {
				return err
			}
			segUsed = flat
		}
	}
	if len(runs) == 0 {
		return fmt.Errorf("no backups under oss prefix %s (new layout) or legacy %s",
			TenantOSSPrefix(r.cfg, tenantSeg, pathSeg), TenantOSSPrefix(r.cfg, tenantSeg, strings.TrimSpace(partitionCreatedAtInput)))
	}

	picked := strings.TrimSpace(runID)
	if picked == "" {
		picked = runs[len(runs)-1]
	}

	remoteRun := filepath.ToSlash(filepath.Join(r.cfg.RemotePrefix, tenantSeg, segUsed, picked))
	localRoot := filepath.Join(r.cfg.StagingDir, "_tenant_restore", tenantSeg, segUsed, picked)
	_ = os.RemoveAll(localRoot)
	if err := os.MkdirAll(localRoot, 0o750); err != nil {
		return err
	}
	if err := r.DownloadRemotePrefix(ctx, remoteRun, localRoot); err != nil {
		return err
	}
	mf := filepath.Join(localRoot, "manifest.json")
	if _, err := os.Stat(mf); err != nil {
		return fmt.Errorf("missing manifest after download: %w", err)
	}
	return r.tools.RestoreDir(ctx, localRoot, drop)
}

// legacyFlatDateInput is true when input is exactly YYYY-MM-DD (older OSS layout used this as path segment).
func legacyFlatDateInput(raw string) (string, bool) {
	s := strings.TrimSpace(raw)
	if len(s) != 10 {
		return "", false
	}
	if _, err := time.Parse("2006-01-02", s); err != nil {
		return "", false
	}
	return s, true
}
