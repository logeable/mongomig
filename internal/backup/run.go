package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/logeable/mongomig/internal/config"
	"github.com/logeable/mongomig/internal/execwrap"
	"github.com/logeable/mongomig/internal/storage"
)

// Runner orchestrates dump / manifest / optional remote upload.
type Runner struct {
	cfg    *config.Root
	tools  *execwrap.Tools
	remote storage.Backend
}

func NewRunner(cfg *config.Root, remote storage.Backend) *Runner {
	if remote == nil {
		remote = storage.NopRemote{}
	}
	return &Runner{cfg: cfg, tools: execwrap.NewTools(cfg), remote: remote}
}

func (r *Runner) RunFull(ctx context.Context, chain string) (*Manifest, error) {
	if err := r.cfg.EnsureStaging(); err != nil {
		return nil, err
	}
	id := time.Now().UTC().Format("20060102T150405.000000000Z")
	outDir := filepath.Join(r.cfg.StagingDir, chain, id)
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return nil, err
	}
	if err := r.tools.DumpFull(ctx, outDir, true); err != nil {
		return nil, err
	}
	oplogPath := filepath.Join(outDir, "oplog.bson")
	if r.cfg.Gzip {
		oplogPath += ".gz"
	}
	maxTS, err := MaxOplogTSInDump(oplogPath, r.cfg.Gzip)
	if err != nil {
		return nil, fmt.Errorf("scan full oplog: %w", err)
	}
	m := &Manifest{
		BackupID:    id,
		CreatedAt:   time.Now().UTC(),
		Type:        TypeFull,
		LastOplogTS: OplogTSFromPrimitive(maxTS),
	}
	if err := WriteManifest(filepath.Join(outDir, "manifest.json"), m); err != nil {
		return nil, err
	}
	if err := saveChainState(r.cfg.StagingDir, chain, m); err != nil {
		return nil, err
	}
	if r.cfg.S3Enabled() {
		if err := r.uploadTree(ctx, chain, id, outDir); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (r *Runner) RunIncremental(ctx context.Context, chain string) (*Manifest, error) {
	if err := r.cfg.EnsureStaging(); err != nil {
		return nil, err
	}
	st, err := loadChainState(r.cfg.StagingDir, chain)
	if err != nil {
		return nil, err
	}
	if st == nil || st.LastOplogTS == nil {
		return nil, fmt.Errorf("no chain state for %q: run full backup first", chain)
	}
	last := st.LastOplogTS.ToPrimitive()
	if err := CheckOplogWindow(ctx, r.cfg.MongoURI, last); err != nil {
		return nil, err
	}
	id := time.Now().UTC().Format("20060102T150405.000000000Z")
	outDir := filepath.Join(r.cfg.StagingDir, chain, id)
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return nil, err
	}
	queryFile := filepath.Join(outDir, "_oplog_query.json")
	if err := WriteOplogQueryFile(queryFile, last); err != nil {
		return nil, err
	}
	if err := r.tools.DumpOplogRange(ctx, outDir, queryFile); err != nil {
		return nil, err
	}
	_ = os.Remove(queryFile)

	src := oplogDumpPath(outDir, r.cfg.Gzip)
	if _, statErr := os.Stat(src); os.IsNotExist(statErr) {
		// No new oplog operations; keep chain state unchanged but record empty increment.
		m := &Manifest{
			BackupID:    id,
			CreatedAt:   time.Now().UTC(),
			Type:        TypeIncr,
			LastOplogTS: st.LastOplogTS,
			Notes:       "no oplog documents matched; chain not advanced",
		}
		if err := WriteManifest(filepath.Join(outDir, "manifest.json"), m); err != nil {
			return nil, err
		}
		if r.cfg.S3Enabled() {
			if err := r.uploadTree(ctx, chain, id, outDir); err != nil {
				return nil, err
			}
		}
		return m, nil
	}

	if err := promoteOplogForReplay(outDir, r.cfg.Gzip); err != nil {
		return nil, err
	}
	replayPath := filepath.Join(outDir, "oplog.bson")
	if r.cfg.Gzip {
		replayPath += ".gz"
	}
	maxTS, err := MaxOplogTSInDump(replayPath, r.cfg.Gzip)
	if err != nil {
		return nil, fmt.Errorf("scan incr oplog: %w", err)
	}
	m := &Manifest{
		BackupID:    id,
		CreatedAt:   time.Now().UTC(),
		Type:        TypeIncr,
		LastOplogTS: OplogTSFromPrimitive(maxTS),
	}
	if err := WriteManifest(filepath.Join(outDir, "manifest.json"), m); err != nil {
		return nil, err
	}
	if err := saveChainState(r.cfg.StagingDir, chain, m); err != nil {
		return nil, err
	}
	if r.cfg.S3Enabled() {
		if err := r.uploadTree(ctx, chain, id, outDir); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func oplogDumpPath(outDir string, gzip bool) string {
	p := filepath.Join(outDir, "local", "oplog.rs.bson")
	if gzip {
		return p + ".gz"
	}
	return p
}

func promoteOplogForReplay(outDir string, gzip bool) error {
	src := oplogDumpPath(outDir, gzip)
	dst := filepath.Join(outDir, "oplog.bson")
	if gzip {
		dst += ".gz"
	}
	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("promote oplog for replay: %w", err)
	}
	_ = os.RemoveAll(filepath.Join(outDir, "local"))
	return nil
}

func (r *Runner) uploadTree(ctx context.Context, chain, backupID, root string) error {
	prefix := filepath.ToSlash(filepath.Join(r.cfg.RemotePrefix, chain, backupID))
	return walkUpload(ctx, r.remote, root, prefix)
}

func walkUpload(ctx context.Context, b storage.Backend, root, keyPrefix string) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		key := keyPrefix + "/" + filepath.ToSlash(rel)
		return b.UploadFile(ctx, path, key)
	})
}
