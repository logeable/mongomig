package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/logeable/mongomig/internal/config"
	"github.com/logeable/mongomig/internal/execwrap"
	"github.com/logeable/mongomig/internal/storage"
)

// RestoreRunner downloads (if needed) and applies manifests in order.
type RestoreRunner struct {
	cfg    *config.Root
	tools  *execwrap.Tools
	remote storage.Backend
}

func NewRestoreRunner(cfg *config.Root, remote storage.Backend) *RestoreRunner {
	if remote == nil {
		remote = storage.NopRemote{}
	}
	return &RestoreRunner{cfg: cfg, tools: execwrap.NewTools(cfg), remote: remote}
}

type restorePlan struct {
	dir      string
	manifest *Manifest
}

// RestoreChain restores from local staging path stagingDir/chain or after remote download into that tree.
func (r *RestoreRunner) RestoreChain(ctx context.Context, chain string, drop bool) error {
	if err := r.cfg.EnsureStaging(); err != nil {
		return err
	}
	root := filepath.Join(r.cfg.StagingDir, chain)
	if r.cfg.S3Enabled() {
		prefix := filepath.ToSlash(filepath.Join(r.cfg.RemotePrefix, chain))
		if err := r.downloadPrefix(ctx, prefix, root); err != nil {
			return err
		}
	}
	plans, err := discoverArtifacts(root)
	if err != nil {
		return err
	}
	if len(plans) == 0 {
		return fmt.Errorf("no backups under %s", root)
	}
	// First must be full.
	if plans[0].manifest.Type != TypeFull {
		return fmt.Errorf("first artifact must be full backup, got %s", plans[0].manifest.Type)
	}
	for i, p := range plans {
		if p.manifest.Type == TypeFull {
			// Only the first full restore should drop target collections.
			useDrop := drop && i == 0
			if err := r.tools.RestoreOplogReplay(ctx, p.dir, useDrop); err != nil {
				return fmt.Errorf("restore full %s: %w", p.manifest.BackupID, err)
			}
			continue
		}
		if p.manifest.Type == TypeIncr {
			// Skip no-op increments that did not produce oplog.bson
			op := filepath.Join(p.dir, "oplog.bson")
			if r.cfg.Gzip {
				op += ".gz"
			}
			if _, err := os.Stat(op); os.IsNotExist(err) {
				continue
			}
			if err := r.tools.RestoreOplogReplay(ctx, p.dir, false); err != nil {
				return fmt.Errorf("restore incr %s: %w", p.manifest.BackupID, err)
			}
			continue
		}
		return fmt.Errorf("unknown backup type %q", p.manifest.Type)
	}
	return nil
}

func (r *RestoreRunner) downloadPrefix(ctx context.Context, remotePrefix, localRoot string) error {
	keys, err := r.remote.ListKeys(ctx, remotePrefix+"/")
	if err != nil {
		return err
	}
	for _, key := range keys {
		rel, ok := cutPrefix(key, remotePrefix+"/")
		if !ok {
			rel = filepath.Base(key)
		}
		dst := filepath.Join(localRoot, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return err
		}
		if err := r.remote.DownloadFile(ctx, key, dst); err != nil {
			return fmt.Errorf("download %s: %w", key, err)
		}
	}
	return nil
}

func cutPrefix(s, prefix string) (string, bool) {
	if len(s) < len(prefix) || s[:len(prefix)] != prefix {
		return "", false
	}
	return s[len(prefix):], true
}

func discoverArtifacts(chainRoot string) ([]restorePlan, error) {
	ents, err := os.ReadDir(chainRoot)
	if err != nil {
		return nil, err
	}
	var plans []restorePlan
	for _, e := range ents {
		if !e.IsDir() || e.Name() == "" || e.Name()[0] == '_' {
			continue
		}
		dir := filepath.Join(chainRoot, e.Name())
		mf := filepath.Join(dir, "manifest.json")
		m, err := ReadManifest(mf)
		if err != nil {
			continue
		}
		plans = append(plans, restorePlan{dir: dir, manifest: m})
	}
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].manifest.CreatedAt.Before(plans[j].manifest.CreatedAt)
	})
	return plans, nil
}

// Status prints chain state and optional remote listing (caller handles IO).
func ChainStatus(cfg *config.Root, chain string) (string, error) {
	st, err := loadChainState(cfg.StagingDir, chain)
	if err != nil {
		return "", err
	}
	if st == nil {
		return fmt.Sprintf("chain %q: no local state", chain), nil
	}
	return fmt.Sprintf("chain %q: last=%s type=%s ts=%v", chain, st.LastBackupID, st.LastType, st.LastOplogTS), nil
}
