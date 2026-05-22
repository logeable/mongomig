package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"

	"github.com/logeable/mongomig/internal/config"
	"github.com/logeable/mongomig/internal/execwrap"
	"github.com/logeable/mongomig/internal/shutdown"
	"github.com/logeable/mongomig/internal/storage"
)

// HourlyRestoreOpts configures RunHourlyOSSRestore.
type HourlyRestoreOpts struct {
	Collections          []NSSpec
	TenantField          string
	TimeField            string
	TenantNumeric        bool
	FromHour             *HourBucket
	ToHour               *HourBucket
	RemotePrefix         string
	CheckpointCollection string
	Drop                 bool
	DryRun               bool
	ResetCheckpoint      bool
	Shutdown             *shutdown.Coordinator
}

// RunHourlyOSSRestore downloads tenant dump.tar objects from OSS and mongorestore's them in UTC hour order.
func RunHourlyOSSRestore(ctx context.Context, cfg *config.Root, remote storage.Backend, log *zap.Logger, opts HourlyRestoreOpts) error {
	if !cfg.S3Enabled() {
		return fmt.Errorf("S3/OSS must be configured for hourly restore")
	}
	meta, err := NewMetaStore(remote, cfg.StagingDir)
	if err != nil {
		return err
	}
	cpStore, err := NewRestoreCheckpointStore(ctx, cfg.MongoURI, opts.CheckpointCollection)
	if err != nil {
		return err
	}
	defer cpStore.Disconnect(context.Background())

	tools := execwrap.NewTools(cfg)
	dropped := make(map[string]bool)
	indexesEnsured := make(map[string]bool)

	for _, ns := range opts.Collections {
		if stop, err := shouldStopWork(ctx, opts.Shutdown); stop {
			return err
		}
		collBase := CollectionBase(opts.RemotePrefix, ns.DB, ns.Coll)
		log.Info("collection restore start", zap.String("collection", ns.String()), zap.String("base", collBase))

		if opts.ResetCheckpoint {
			if err := cpStore.Remove(ctx, opts.RemotePrefix, ns); err != nil {
				return err
			}
			log.Info("restore checkpoint reset",
				zap.String("collection", ns.String()),
				zap.String("checkpoint_coll", cpStore.collName),
			)
		}

		cp, err := cpStore.Load(ctx, opts.RemotePrefix, collBase, cfg.MongoURI, ns)
		if err != nil {
			return fmt.Errorf("%s: %w", ns.String(), err)
		}
		if cp != nil {
			log.Info("restore checkpoint loaded",
				zap.String("collection", ns.String()),
				zap.String("checkpoint_db", ns.DB),
				zap.String("checkpoint_coll", cpStore.collName),
				zap.String("checkpoint_id", cp.ID),
				zap.String("newest_restored", hourRefLabel(cp.NewestRestored)),
			)
		}

		collMeta, _, err := meta.LoadCollectionMeta(ctx, collBase)
		if err != nil {
			return err
		}
		start, end, err := resolveRestoreHourRange(opts, collMeta, cp)
		if err != nil {
			return fmt.Errorf("%s: %w", ns.String(), err)
		}
		hours := HoursInclusive(start, end)
		log.Info("restore hour range",
			zap.String("collection", ns.String()),
			zap.String("from_hour", start.String()),
			zap.String("to_hour", end.String()),
			zap.Int("hour_count", len(hours)),
		)
		if len(hours) == 0 {
			log.Info("restore up to date", zap.String("collection", ns.String()))
			continue
		}

		if cp == nil {
			cp = newRestoreCheckpoint(opts.RemotePrefix, collBase, cfg.MongoURI, ns)
		}

		// When not using --drop on first tenant, ensure indexes before loading data.
		skipDrop := cp.NewestRestored != nil
		if !opts.DryRun && (!opts.Drop || skipDrop) {
			if err := ensureCollectionIndexesOnce(ctx, cfg, meta, log, ns, collBase, indexesEnsured); err != nil {
				return fmt.Errorf("%s indexes: %w", ns.String(), err)
			}
		} else if opts.DryRun {
			log.Info("dry-run: would ensure indexes from OSS indexes.json",
				zap.String("collection", ns.String()),
				zap.String("key", CollectionIndexesKey(collBase)),
			)
		}

		for _, hb := range hours {
			if stop, err := shouldStopWork(ctx, opts.Shutdown); stop {
				return err
			}
			if err := restoreHour(ctx, cfg, remote, meta, cpStore, cp, tools, log, opts, ns, collBase, hb, dropped, indexesEnsured); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				return fmt.Errorf("%s hour %s: %w", ns.String(), hb.String(), err)
			}
		}
	}
	return nil
}

func resolveRestoreHourRange(opts HourlyRestoreOpts, collMeta *CollectionMeta, cp *RestoreCheckpoint) (start, end HourBucket, err error) {
	if opts.FromHour != nil {
		start = *opts.FromHour
	} else if cp != nil && cp.NewestRestored != nil {
		start = cp.NewestRestored.Bucket().Next()
	} else if collMeta != nil && collMeta.OldestCompleted != nil {
		start = collMeta.OldestCompleted.Bucket()
	} else {
		return HourBucket{}, HourBucket{}, fmt.Errorf("set --from-hour or ensure OSS collection meta has oldest_completed")
	}
	if opts.ToHour != nil {
		end = *opts.ToHour
	} else {
		var endSet bool
		if collMeta != nil && collMeta.NewestCompleted != nil {
			end = collMeta.NewestCompleted.Bucket()
			endSet = true
		}
		if collMeta != nil && collMeta.Active != nil {
			if err := validateHourStatus(collMeta.Active.Status, "collection active"); err != nil {
				return HourBucket{}, HourBucket{}, err
			}
			active := collMeta.Active.Bucket()
			if !endSet || active.Start.After(end.Start) {
				end = active
				endSet = true
			}
		}
		if !endSet {
			return HourBucket{}, HourBucket{}, fmt.Errorf("set --to-hour or ensure OSS collection meta has newest_completed or active")
		}
	}
	if end.Start.Before(start.Start) {
		return start, end, nil
	}
	return start, end, nil
}

func restoreHour(
	ctx context.Context,
	cfg *config.Root,
	remote storage.Backend,
	meta *MetaStore,
	cpStore *RestoreCheckpointStore,
	cp *RestoreCheckpoint,
	tools *execwrap.Tools,
	log *zap.Logger,
	opts HourlyRestoreOpts,
	ns NSSpec,
	collBase string,
	hb HourBucket,
	dropped map[string]bool,
	indexesEnsured map[string]bool,
) error {
	if cp != nil && cp.hourAlreadyRestored(hb) {
		log.Debug("skip hour: already restored", zap.String("hour", hb.String()))
		return nil
	}

	hourBase := HourBase(collBase, hb)
	hm, ok, err := meta.LoadHourMeta(ctx, hourBase)
	if err != nil {
		return err
	}
	if !ok {
		log.Warn("skip hour: meta.json missing on OSS", zap.String("hour", hb.String()))
		return nil
	}
	if err := validateHourStatus(hm.Status, HourMetaKey(hourBase)); err != nil {
		return err
	}
	hourComplete := hm.Status == HourStatusComplete
	tenants := tenantsToRestore(hm.Tenants)
	if len(tenants) == 0 {
		log.Warn("skip hour: no uploaded tenants", zap.String("hour", hb.String()), zap.String("status", string(hm.Status)))
		return nil
	}
	log.Info("restore hour",
		zap.String("collection", ns.String()),
		zap.String("hour", hb.String()),
		zap.String("status", string(hm.Status)),
		zap.Bool("hour_complete_on_oss", hourComplete),
		zap.Int("tenant_count", len(tenants)),
		zap.Int("tenant_total", len(hm.Tenants)),
	)
	if !hourComplete {
		log.Info("restore partial hour: only uploaded tenants; checkpoint advances after OSS hour is complete",
			zap.String("hour", hb.String()),
		)
	}
	for _, row := range tenants {
		if stop, _ := shouldStopWork(ctx, opts.Shutdown); stop {
			return persistRestoreShutdown(log, opts, ns, hb)
		}
		if err := restoreOneTenant(ctx, cfg, remote, meta, cp, tools, log, opts, ns, collBase, hourBase, hb, row, dropped, indexesEnsured, opts.TenantField, opts.TimeField, opts.TenantNumeric); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return persistRestoreShutdown(log, opts, ns, hb)
			}
			return err
		}
	}
	if stop, _ := shouldStopWork(ctx, opts.Shutdown); stop {
		return persistRestoreShutdown(log, opts, ns, hb)
	}
	if !opts.DryRun && hourComplete {
		cp.markHourComplete(hb)
		if err := cpStore.Save(ctx, cp, ns); err != nil {
			return err
		}
		if opts.Shutdown != nil {
			opts.Shutdown.NotifyPersisted()
		}
	}
	return nil
}

// persistRestoreShutdown returns without advancing newest_restored (interrupted hour will be retried whole-hour).
func persistRestoreShutdown(log *zap.Logger, opts HourlyRestoreOpts, ns NSSpec, hb HourBucket) error {
	if opts.Shutdown != nil {
		opts.Shutdown.NotifyPersisted()
	}
	log.Warn("shutdown requested, restore checkpoint unchanged (retry whole hour on next run)",
		zap.String("collection", ns.String()),
		zap.String("hour", hb.String()),
		zap.Bool("forced", opts.Shutdown != nil && opts.Shutdown.Forced()),
	)
	return context.Canceled
}

func tenantsToRestore(rows []TenantMetaRow) []TenantMetaRow {
	var out []TenantMetaRow
	for _, row := range rows {
		if !row.Uploaded || row.Error != "" {
			continue
		}
		if strings.TrimSpace(row.DataRelPath) == "" {
			continue
		}
		out = append(out, row)
	}
	return out
}

func restoreOneTenant(
	ctx context.Context,
	cfg *config.Root,
	remote storage.Backend,
	meta *MetaStore,
	cp *RestoreCheckpoint,
	tools *execwrap.Tools,
	log *zap.Logger,
	opts HourlyRestoreOpts,
	ns NSSpec,
	collBase string,
	hourBase string,
	hb HourBucket,
	row TenantMetaRow,
	dropped map[string]bool,
	indexesEnsured map[string]bool,
	tenantField, timeField string,
	tenantNumeric bool,
) error {
	ossKey := TenantObjectKey(hourBase, row.DataRelPath)
	workDir := cfg.AbsStaging("restore", ns.DB, ns.Coll, hb.String(), row.TenantKey)
	tarPath := filepath.Join(workDir, "dump.tar")
	extractDir := filepath.Join(workDir, "extract")

	skipDrop := cp != nil && cp.NewestRestored != nil
	if opts.DryRun {
		drop := opts.Drop && !dropped[ns.String()] && !skipDrop
		log.Info("dry-run: would delete tenant hour slice and restore",
			zap.String("collection", ns.String()),
			zap.String("hour", hb.String()),
			zap.String("tenant", row.TenantKey),
			zap.String("oss_key", ossKey),
			zap.Bool("drop_collection", drop),
		)
		return nil
	}

	tenantCtx := ctx
	if opts.Shutdown != nil {
		tenantCtx = opts.Shutdown.TenantContext()
	}

	defer func() {
		if err := os.RemoveAll(workDir); err != nil && log != nil {
			log.Debug("cleanup restore work dir", zap.String("work_dir", workDir), zap.Error(err))
		}
	}()
	if err := os.RemoveAll(workDir); err != nil {
		return err
	}
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		return err
	}
	log.Debug("downloading dump.tar", zap.String("oss_key", ossKey), zap.String("local", tarPath))
	if err := remote.DownloadFile(tenantCtx, ossKey, tarPath); err != nil {
		return fmt.Errorf("download %s: %w", ossKey, err)
	}
	if row.SHA256 != "" {
		got, _, err := fileSHA256(tarPath)
		if err != nil {
			return err
		}
		if !strings.EqualFold(got, row.SHA256) {
			return fmt.Errorf("sha256 mismatch for %s: oss meta %s, local %s", ossKey, row.SHA256, got)
		}
	}
	if err := untarDirectory(tenantCtx, tarPath, extractDir); err != nil {
		return err
	}
	dumpRoot := filepath.Join(extractDir, "dump_out")
	if _, err := DeleteTenantHourSlice(tenantCtx, cfg.MongoURI, log, ns, tenantField, row.TenantKey, tenantNumeric, timeField, hb); err != nil {
		return err
	}
	drop := opts.Drop && !dropped[ns.String()] && !skipDrop
	log.Info("mongorestore tenant",
		zap.String("collection", ns.String()),
		zap.String("hour", hb.String()),
		zap.String("tenant", row.TenantKey),
		zap.Bool("drop_collection", drop),
	)
	if err := tools.RestoreNamespace(tenantCtx, dumpRoot, ns.DB, ns.Coll, drop); err != nil {
		return err
	}
	if drop {
		dropped[ns.String()] = true
	}
	return ensureCollectionIndexesOnce(ctx, cfg, meta, log, ns, collBase, indexesEnsured)
}
