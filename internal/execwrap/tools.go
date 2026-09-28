package execwrap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/logeable/mongomig/internal/config"
)

// Tools runs mongodump / mongorestore with timeouts and streaming stderr.
type Tools struct {
	cfg *config.Root
}

func NewTools(cfg *config.Root) *Tools {
	return &Tools{cfg: cfg}
}

func (t *Tools) resolveBin(name, override string) (string, error) {
	if override != "" {
		if _, err := os.Stat(override); err == nil {
			return filepath.Abs(override)
		}
		return "", fmt.Errorf("%s not found at %q", name, override)
	}
	p, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("look up %s in PATH: %w", name, err)
	}
	return p, nil
}

func (t *Tools) run(ctx context.Context, name string, args []string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 24 * time.Hour
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	var stderr bytes.Buffer
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	cmd.Stdout = os.Stdout

	if err := cmd.Run(); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%s timed out after %s: %w", filepath.Base(name), timeout, err)
		}
		return fmt.Errorf("%s %v: %w\nstderr: %s", filepath.Base(name), args, err, stderr.String())
	}
	return nil
}

// DumpFull runs mongodump for entire deployment with optional oplog tail capture.
func (t *Tools) DumpFull(ctx context.Context, outDir string, withOplog bool) error {
	bin, err := t.resolveBin("mongodump", t.cfg.MongodumpPath)
	if err != nil {
		return err
	}
	args := []string{"--uri", t.cfg.MongoURI, "--out", outDir}
	if withOplog {
		args = append(args, "--oplog")
	}
	if t.cfg.Gzip {
		args = append(args, "--gzip")
	}
	if t.cfg.ParallelCollections > 0 {
		args = append(args, "--numParallelCollections", fmt.Sprintf("%d", t.cfg.ParallelCollections))
	}
	return t.run(ctx, bin, args, t.cfg.DumpTimeout)
}

// DumpOplogRange dumps local.oplog.rs entries with ts > after (exclusive) using query file.
func (t *Tools) DumpOplogRange(ctx context.Context, outDir, queryFile string) error {
	bin, err := t.resolveBin("mongodump", t.cfg.MongodumpPath)
	if err != nil {
		return err
	}
	args := []string{
		"--uri", t.cfg.MongoURI,
		"--db", "local",
		"--collection", "oplog.rs",
		"--queryFile", queryFile,
		"--out", outDir,
	}
	if t.cfg.Gzip {
		args = append(args, "--gzip")
	}
	return t.run(ctx, bin, args, t.cfg.DumpTimeout)
}

// DumpCollectionQuery runs mongodump for one collection with --queryFile.
func (t *Tools) DumpCollectionQuery(ctx context.Context, db, coll, outDir, queryFile string) error {
	bin, err := t.resolveBin("mongodump", t.cfg.MongodumpPath)
	if err != nil {
		return err
	}
	args := []string{
		"--uri", t.cfg.MongoURI,
		"--db", db,
		"--collection", coll,
		"--queryFile", queryFile,
		"--out", outDir,
	}
	if t.cfg.Gzip {
		args = append(args, "--gzip")
	}
	return t.run(ctx, bin, args, t.cfg.DumpTimeout)
}

// RestoreNamespace restores one collection from a mongodump output tree (dumpDir/{db}/{coll}.bson[.gz]).
func (t *Tools) RestoreNamespace(ctx context.Context, dumpDir, db, coll string, drop bool) error {
	bin, err := t.resolveBin("mongorestore", t.cfg.MongorestorePath)
	if err != nil {
		return err
	}
	bsonPath, err := resolveDumpBSONPath(dumpDir, db, coll, t.cfg.Gzip)
	if err != nil {
		return err
	}
	args := restoreNamespaceArgs(t.cfg.MongoURI, db, coll, t.cfg.Gzip, drop)
	args = append(args, bsonPath)
	return t.run(ctx, bin, args, t.cfg.RestoreTimeout)
}

func restoreNamespaceArgs(uri, db, coll string, gzip, drop bool) []string {
	args := []string{"--uri", uri, "--db", db, "--collection", coll}
	if gzip {
		args = append(args, "--gzip")
	}
	// Indexes are managed by mongomig from indexes.json. This also prevents a
	// tenant-scoped restore from applying the dump metadata's indexes to the
	// entire target collection.
	args = append(args, "--noIndexRestore")
	if drop {
		args = append(args, "--drop")
	}
	return args
}

func resolveDumpBSONPath(dumpDir, db, coll string, gzip bool) (string, error) {
	if gzip {
		p := filepath.Join(dumpDir, db, coll+".bson.gz")
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	p := filepath.Join(dumpDir, db, coll+".bson")
	if _, err := os.Stat(p); err == nil {
		return p, nil
	}
	return "", fmt.Errorf("dump bson not found under %s for %s.%s", dumpDir, db, coll)
}

// RestoreDir runs mongorestore from a dump directory (no oplog replay).
func (t *Tools) RestoreDir(ctx context.Context, dumpDir string, drop bool) error {
	bin, err := t.resolveBin("mongorestore", t.cfg.MongorestorePath)
	if err != nil {
		return err
	}
	args := []string{"--uri", t.cfg.MongoURI}
	if t.cfg.Gzip {
		args = append(args, "--gzip")
	}
	if drop {
		args = append(args, "--drop")
	}
	args = append(args, dumpDir)
	return t.run(ctx, bin, args, t.cfg.RestoreTimeout)
}

// RestoreOplogReplay replays oplog.bson under dumpDir (must contain oplog.bson at root).
// drop applies --drop only when true (typically first full restore).
func (t *Tools) RestoreOplogReplay(ctx context.Context, dumpDir string, drop bool) error {
	bin, err := t.resolveBin("mongorestore", t.cfg.MongorestorePath)
	if err != nil {
		return err
	}
	args := []string{"--uri", t.cfg.MongoURI, "--oplogReplay"}
	if t.cfg.Gzip {
		args = append(args, "--gzip")
	}
	if drop {
		args = append(args, "--drop")
	}
	args = append(args, dumpDir)
	return t.run(ctx, bin, args, t.cfg.RestoreTimeout)
}
