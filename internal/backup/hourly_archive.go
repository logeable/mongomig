package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/logeable/mongomig/internal/config"
	"github.com/logeable/mongomig/internal/execwrap"
)

// TenantHourArchive runs mongodump for one tenant in [start,end) and packs staging dir into dump.tar.
type TenantHourArchive struct {
	cfg   *config.Root
	tools *execwrap.Tools
}

func NewTenantHourArchive(cfg *config.Root) *TenantHourArchive {
	return &TenantHourArchive{cfg: cfg, tools: execwrap.NewTools(cfg)}
}

// DumpAndTar returns path to dump.tar, sha256 hex, size bytes.
func (a *TenantHourArchive) DumpAndTar(ctx context.Context, log *zap.Logger, ns NSSpec, tenantField, tenantKey string, tenantNumeric bool, timeField string, startUTC, endUTC time.Time, workDir string) (tarPath, sha string, size int64, err error) {
	if log != nil {
		log.Debug("mongodump prepare",
			zap.String("tenant", tenantKey),
			zap.String("collection", ns.String()),
			zap.Time("start_utc", startUTC),
			zap.Time("end_utc", endUTC),
			zap.String("work_dir", workDir),
		)
	}
	if err := os.RemoveAll(workDir); err != nil {
		return "", "", 0, err
	}
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return "", "", 0, err
	}
	queryFile := filepath.Join(workDir, "query.json")
	if err := WriteTenantDayRangeQueryFile(queryFile, tenantField, tenantKey, tenantNumeric, timeField, startUTC, endUTC); err != nil {
		return "", "", 0, err
	}
	if log != nil {
		log.Debug("mongodump query file written", zap.String("path", queryFile))
	}
	stagingOut := filepath.Join(workDir, "dump_out")
	if err := a.tools.DumpCollectionQuery(ctx, ns.DB, ns.Coll, stagingOut, queryFile); err != nil {
		return "", "", 0, err
	}
	if log != nil {
		log.Debug("mongodump finished", zap.String("out", stagingOut))
	}
	tarPath = filepath.Join(workDir, "dump.tar")
	if err := tarDirectory(ctx, stagingOut, tarPath); err != nil {
		return "", "", 0, err
	}
	h, size, err := fileSHA256(tarPath)
	if err != nil {
		return "", "", 0, err
	}
	if log != nil {
		log.Debug("dump.tar ready", zap.String("path", tarPath), zap.Int64("size_bytes", size), zap.String("sha256", h))
	}
	return tarPath, h, size, nil
}

func tarDirectory(ctx context.Context, stagingRoot, tarPath string) error {
	base := filepath.Base(stagingRoot)
	parent := filepath.Dir(stagingRoot)
	cmd := exec.CommandContext(ctx, "tar", "-cf", tarPath, "-C", parent, base)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("tar -cf: %w", err)
	}
	return nil
}

func untarDirectory(ctx context.Context, tarPath, destDir string) error {
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "tar", "-xf", tarPath, "-C", destDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("tar -xf: %w", err)
	}
	return nil
}

func fileSHA256(path string) (hexDigest string, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
