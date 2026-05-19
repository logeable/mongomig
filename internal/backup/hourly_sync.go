package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/logeable/mongomig/internal/config"
	"github.com/logeable/mongomig/internal/shutdown"
	"github.com/logeable/mongomig/internal/storage"
)

// HourlySyncOpts configures RunHourlyOSSSync.
type HourlySyncOpts struct {
	Collections    []NSSpec
	TenantField    string
	TimeField      string
	TenantNumeric  bool
	FromHour       *HourBucket
	ToHour         HourBucket
	CleanupLocal   bool
	DryRun         bool
	ForceHour      bool
	RemotePrefix   string
	Shutdown       *shutdown.Coordinator
}

// RunHourlyOSSSync backs up each collection by UTC hour buckets to OSS.
func RunHourlyOSSSync(ctx context.Context, cfg *config.Root, remote storage.Backend, log *zap.Logger, opts HourlySyncOpts) error {
	if !cfg.S3Enabled() {
		return fmt.Errorf("S3/OSS must be configured for hourly backup")
	}
	log.Debug("hourly sync start",
		zap.String("remote_prefix", opts.RemotePrefix),
		zap.String("to_hour", opts.ToHour.String()),
		zap.Bool("dry_run", opts.DryRun),
		zap.Bool("force_hour", opts.ForceHour),
		zap.Int("collection_count", len(opts.Collections)),
	)
	if opts.FromHour != nil {
		log.Debug("hourly sync from_hour override", zap.String("from_hour", opts.FromHour.String()))
	}

	meta, err := NewMetaStore(remote, cfg.StagingDir)
	if err != nil {
		return err
	}
	archiver := NewTenantHourArchive(cfg)
	now := time.Now().UTC()
	log.Debug("sync clock", zap.Time("now_utc", now))

	for _, ns := range opts.Collections {
		if stop, err := shouldStopWork(ctx, opts.Shutdown); stop {
			return err
		}
		collBase := CollectionBase(opts.RemotePrefix, ns.DB, ns.Coll)
		log.Info("collection backup start", zap.String("collection", ns.String()), zap.String("base", collBase))

		if err := WriteCollectionIndexesJSON(ctx, cfg.MongoURI, remote, meta, collBase, ns); err != nil {
			return fmt.Errorf("%s indexes: %w", ns.String(), err)
		}
		log.Debug("indexes.json uploaded", zap.String("key", CollectionIndexesKey(collBase)))

		collMeta, collFound, err := meta.LoadCollectionMeta(ctx, collBase)
		if err != nil {
			return err
		}
		if collMeta == nil {
			collMeta = &CollectionMeta{DB: ns.DB, Collection: ns.Coll}
		}
		collMeta.DB = ns.DB
		collMeta.Collection = ns.Coll
		if collMeta.Active != nil {
			if err := validateHourStatus(collMeta.Active.Status, "collection active"); err != nil {
				return err
			}
		}
		log.Debug("collection meta loaded",
			zap.Bool("found", collFound),
			zap.String("meta_key", CollectionMetaKey(collBase)),
		)
		logCollectionMeta(log, collMeta)
		warnCompletedGap(log, collMeta)

		startHour, err := resolveStartHour(ctx, cfg.MongoURI, ns, opts, collMeta)
		if err != nil {
			return err
		}
		log.Debug("start hour resolved",
			zap.String("start_hour", startHour.String()),
			zap.Bool("from_flag", opts.FromHour != nil),
			zap.Bool("from_newest_completed", collMeta.NewestCompleted != nil),
		)
		endHour := opts.ToHour
		if opts.FromHour != nil {
			startHour = *opts.FromHour
		}
		hourList := HoursInclusive(startHour, endHour)
		log.Debug("hour range planned",
			zap.String("start_hour", startHour.String()),
			zap.String("end_hour", endHour.String()),
			zap.Int("hour_count", len(hourList)),
		)
		for _, hb := range hourList {
			if stop, err := shouldStopWork(ctx, opts.Shutdown); stop {
				return err
			}
			hourBase := HourBase(collBase, hb)
			log.Debug("processing hour", zap.String("hour", hb.String()), zap.String("hour_base", hourBase))
			hm, exists, err := meta.LoadHourMeta(ctx, hourBase)
			if err != nil {
				return err
			}
			if exists {
				if err := validateHourStatus(hm.Status, HourMetaKey(hourBase)); err != nil {
					return err
				}
				log.Debug("hour meta loaded",
					zap.String("hour", hb.String()),
					zap.String("status", string(hm.Status)),
					zap.Int("tenants", len(hm.Tenants)),
				)
			} else {
				log.Debug("hour meta missing, will create", zap.String("hour", hb.String()))
			}
			if exists && hm.Status == HourStatusComplete && !opts.ForceHour {
				log.Debug("skip complete hour", zap.String("hour", hb.String()))
				continue
			}
			if exists && hm.Status == HourStatusPartial {
				log.Info("partial hour: wipe and rediscover", zap.String("hour", hb.String()))
				if err := remote.DeletePrefix(ctx, hourBase+"/"); err != nil {
					return err
				}
			}
			if err := processHour(ctx, cfg, remote, meta, archiver, log, opts, ns, collBase, collMeta, hb, now, opts.Shutdown); err != nil {
				return err
			}
		}
		if err := meta.SaveCollectionMeta(ctx, collBase, collMeta); err != nil {
			return err
		}
		log.Debug("collection backup done", zap.String("collection", ns.String()))
	}
	log.Debug("hourly sync finished", zap.Int("collections", len(opts.Collections)))
	return nil
}

// shouldStopWork reports whether to stop before starting new tenants/hours.
func shouldStopWork(ctx context.Context, coord *shutdown.Coordinator) (bool, error) {
	if coord != nil && coord.Stopping() {
		return true, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	return false, nil
}

func logCollectionMeta(log *zap.Logger, cm *CollectionMeta) {
	if cm == nil {
		return
	}
	fields := []zap.Field{zap.Time("updated_at", cm.UpdatedAt)}
	if cm.OldestCompleted != nil {
		fields = append(fields, zap.String("oldest_completed", hourRefLabel(cm.OldestCompleted)))
	}
	if cm.NewestCompleted != nil {
		fields = append(fields, zap.String("newest_completed", hourRefLabel(cm.NewestCompleted)))
	}
	if cm.Active != nil {
		fields = append(fields,
			zap.String("active_hour", hourRefLabel(cm.Active)),
			zap.String("active_status", string(cm.Active.Status)),
		)
	} else {
		fields = append(fields, zap.Bool("active", false))
	}
	log.Debug("collection meta state", fields...)
}

func hourRefLabel(r *HourRef) string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("%04d-%02d-%02dT%02dZ(%s)", r.Year, r.Month, r.Day, r.Hour, r.Status)
}

func countUploaded(rows []TenantMetaRow) int {
	n := 0
	for _, t := range rows {
		if t.Uploaded {
			n++
		}
	}
	return n
}

func sameHour(a, b HourBucket) bool {
	return a.Start.Equal(b.Start)
}

func validateHourStatus(s HourStatus, where string) error {
	switch s {
	case HourStatusPartial, HourStatusComplete:
		return nil
	default:
		return fmt.Errorf("%s: invalid hour status %q (expected partial or complete)", where, s)
	}
}

func warnCompletedGap(log *zap.Logger, cm *CollectionMeta) {
	if cm == nil || cm.NewestCompleted == nil || cm.Active == nil {
		return
	}
	next := cm.NewestCompleted.Bucket().Next()
	active := cm.Active.Bucket()
	if active.Start.After(next.Start) && !sameHour(active, next) {
		log.Warn("backup gap: newest_completed+1 is behind active; forward scan will backfill",
			zap.String("newest_completed", hourRefLabel(cm.NewestCompleted)),
			zap.String("expected_next_hour", next.String()),
			zap.String("active", hourRefLabel(cm.Active)),
		)
	}
}

func resolveStartHour(ctx context.Context, mongoURI string, ns NSSpec, opts HourlySyncOpts, cm *CollectionMeta) (HourBucket, error) {
	if opts.FromHour != nil {
		return *opts.FromHour, nil
	}
	if cm.NewestCompleted != nil {
		return cm.NewestCompleted.Bucket().Next(), nil
	}
	minT, _, ok, err := MinMaxTimeFieldForCollection(ctx, mongoURI, ns, opts.TimeField)
	if err != nil {
		return HourBucket{}, err
	}
	if !ok {
		return HourBucketUTC(time.Now().UTC()), nil
	}
	return HourBucketUTC(minT), nil
}

func processHour(
	ctx context.Context,
	cfg *config.Root,
	remote storage.Backend,
	meta *MetaStore,
	archiver *TenantHourArchive,
	log *zap.Logger,
	opts HourlySyncOpts,
	ns NSSpec,
	collBase string,
	collMeta *CollectionMeta,
	hb HourBucket,
	now time.Time,
	coord *shutdown.Coordinator,
) error {
	hourBase := HourBase(collBase, hb)
	hourMetaKey := HourMetaKey(hourBase)

	if opts.DryRun {
		log.Info("dry-run: would backup hour", zap.String("hour", hb.String()))
		return nil
	}

	log.Debug("discover tenants",
		zap.String("hour", hb.String()),
		zap.Time("interval_start", hb.Start),
		zap.Time("interval_end", hb.End),
		zap.String("tenant_field", opts.TenantField),
		zap.String("time_field", opts.TimeField),
	)
	tenants, err := DiscoverTenantsInHour(ctx, cfg.MongoURI, ns, opts.TenantField, opts.TimeField, opts.TenantNumeric, hb.Start, hb.End)
	if err != nil {
		return err
	}
	log.Debug("discover tenants done", zap.String("hour", hb.String()), zap.Int("count", len(tenants)))

	hm := newHourMeta(ns, hb, HourStatusPartial)
	for _, tk := range tenants {
		row, err := tenantRowForKey(hourBase, tk)
		if err != nil {
			log.Debug("tenant shard path skipped", zap.String("tenant_key", tk), zap.Error(err))
			hm.Tenants = append(hm.Tenants, TenantMetaRow{TenantKey: tk, Error: err.Error()})
			continue
		}
		hm.Tenants = append(hm.Tenants, row)
	}
	log.Debug("hour meta initial save", zap.String("hour_meta_key", hourMetaKey), zap.Int("tenant_rows", len(hm.Tenants)))
	if err := meta.SaveHourMeta(ctx, hourBase, hm); err != nil {
		return err
	}
	setCollectionActive(collMeta, collBase, hb, hourMetaKey, HourStatusPartial)
	if err := meta.SaveCollectionMeta(ctx, collBase, collMeta); err != nil {
		return err
	}

	for i := range hm.Tenants {
		if coord != nil && coord.Stopping() {
			return persistShutdown(meta, log, ns, collBase, collMeta, hourBase, hm, hb, coord)
		}
		if err := ctx.Err(); err != nil {
			return persistShutdown(meta, log, ns, collBase, collMeta, hourBase, hm, hb, coord)
		}
		row := &hm.Tenants[i]
		if row.Error != "" {
			continue
		}
		if err := backupOneTenant(ctx, cfg, remote, meta, archiver, log, opts, ns, collBase, hourBase, hb, hm, row, i, coord); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return persistShutdown(meta, log, ns, collBase, collMeta, hourBase, hm, hb, coord)
			}
			row.Error = err.Error()
			row.Uploaded = false
			_ = meta.SaveHourMeta(ctx, hourBase, hm)
			log.Error("tenant backup failed", zap.String("tenant", row.TenantKey), zap.Error(err))
			continue
		}
		log.Debug("hour meta checkpoint after tenant",
			zap.String("hour", hb.String()),
			zap.String("tenant", row.TenantKey),
			zap.Int("uploaded", countUploaded(hm.Tenants)),
			zap.Int("total", len(hm.Tenants)),
		)
		if err := meta.SaveHourMeta(ctx, hourBase, hm); err != nil {
			return err
		}
	}

	if coord != nil && coord.Stopping() {
		return persistShutdown(meta, log, ns, collBase, collMeta, hourBase, hm, hb, coord)
	}
	if err := ctx.Err(); err != nil {
		return persistShutdown(meta, log, ns, collBase, collMeta, hourBase, hm, hb, coord)
	}

	return finalizeHour(ctx, meta, log, collBase, collMeta, hourBase, hb, hm, now)
}

func newHourMeta(ns NSSpec, hb HourBucket, status HourStatus) *HourMeta {
	return &HourMeta{
		Status:           status,
		IntervalStartUTC: hb.Start.UTC().Format(time.RFC3339Nano),
		IntervalEndUTC:   hb.End.UTC().Format(time.RFC3339Nano),
		Year:             hb.Year,
		Month:            hb.Month,
		Day:              hb.Day,
		Hour:             hb.Hour,
		DB:               ns.DB,
		Collection:       ns.Coll,
	}
}

func tenantRowForKey(hourBase, tenantKey string) (TenantMetaRow, error) {
	shard, err := TenantShard(tenantKey)
	if err != nil {
		return TenantMetaRow{}, err
	}
	rel := TenantDataRelPath(shard, tenantKey)
	return TenantMetaRow{
		TenantKey:   tenantKey,
		Shard:       shard,
		DataRelPath: rel,
	}, nil
}

func backupOneTenant(
	ctx context.Context,
	cfg *config.Root,
	remote storage.Backend,
	meta *MetaStore,
	archiver *TenantHourArchive,
	log *zap.Logger,
	opts HourlySyncOpts,
	ns NSSpec,
	collBase, hourBase string,
	hb HourBucket,
	hm *HourMeta,
	row *TenantMetaRow,
	idx int,
	coord *shutdown.Coordinator,
) error {
	if row.TenantKey == "" {
		return fmt.Errorf("empty tenant_key at index %d", idx)
	}
	shard, err := TenantShard(row.TenantKey)
	if err != nil {
		return err
	}
	rel := TenantDataRelPath(shard, row.TenantKey)
	ossKey := TenantObjectKey(hourBase, rel)
	row.Shard = shard
	row.DataRelPath = rel
	row.Error = ""

	workDir := cfg.AbsStaging("hourly", ns.DB, ns.Coll, hb.String(), row.TenantKey)
	log.Debug("tenant backup start",
		zap.String("collection", ns.String()),
		zap.String("hour", hb.String()),
		zap.String("tenant", row.TenantKey),
		zap.String("shard", shard),
		zap.String("data_rel_path", rel),
		zap.String("work_dir", workDir),
	)
	tenantCtx := ctx
	if coord != nil {
		tenantCtx = coord.TenantContext()
	}
	tarPath, sha, size, err := archiver.DumpAndTar(tenantCtx, log, ns, opts.TenantField, row.TenantKey, opts.TenantNumeric, opts.TimeField, hb.Start, hb.End, workDir)
	if err != nil {
		return err
	}
	log.Debug("uploading dump.tar", zap.String("local", tarPath), zap.Int64("size_bytes", size))
	if err := remote.UploadFile(tenantCtx, tarPath, ossKey); err != nil {
		return err
	}
	row.Uploaded = true
	row.SHA256 = sha
	row.SizeBytes = size
	log.Info("tenant uploaded",
		zap.String("tenant", row.TenantKey),
		zap.String("data_rel_path", rel),
		zap.String("sha256", sha),
	)
	if opts.CleanupLocal {
		log.Debug("cleanup local staging", zap.String("work_dir", workDir))
		_ = os.RemoveAll(workDir)
	}
	return nil
}

func finalizeHour(ctx context.Context, meta *MetaStore, log *zap.Logger, collBase string, collMeta *CollectionMeta, hourBase string, hb HourBucket, hm *HourMeta, now time.Time) error {
	allOK := true
	for _, t := range hm.Tenants {
		if t.Error != "" || !t.Uploaded {
			allOK = false
			break
		}
	}
	hourEnded := !now.Before(hb.End)
	uploaded := countUploaded(hm.Tenants)
	log.Debug("finalize hour",
		zap.String("hour", hb.String()),
		zap.Bool("hour_ended", hourEnded),
		zap.Bool("all_ok", allOK),
		zap.Int("uploaded", uploaded),
		zap.Int("tenants", len(hm.Tenants)),
	)
	if hourEnded && allOK {
		hm.Status = HourStatusComplete
		hm.CompletedAt = now.UTC().Format(time.RFC3339Nano)
		log.Info("hour complete",
			zap.String("hour", hb.String()),
			zap.String("completed_at", hm.CompletedAt),
			zap.Int("tenants", len(hm.Tenants)),
		)
		if err := meta.SaveHourMeta(ctx, hourBase, hm); err != nil {
			return err
		}
		ref := HourRefFromBucket(hb, HourMetaRefRelative(hb), HourStatusComplete)
		updateCompletedBounds(collMeta, ref)
		collMeta.Active = nil
		log.Debug("collection meta updated after complete", zap.String("newest", hourRefLabel(collMeta.NewestCompleted)))
		return meta.SaveCollectionMeta(ctx, collBase, collMeta)
	}
	hm.Status = HourStatusPartial
	log.Info("hour partial",
		zap.String("hour", hb.String()),
		zap.Bool("hour_ended", hourEnded),
		zap.Int("uploaded", uploaded),
		zap.Int("tenants", len(hm.Tenants)),
	)
	setCollectionActive(collMeta, collBase, hb, HourMetaKey(hourBase), HourStatusPartial)
	if err := meta.SaveHourMeta(ctx, hourBase, hm); err != nil {
		return err
	}
	return meta.SaveCollectionMeta(ctx, collBase, collMeta)
}

func updateCompletedBounds(cm *CollectionMeta, ref HourRef) {
	ref.Status = HourStatusComplete
	if cm.OldestCompleted == nil || ref.Bucket().Start.Before(cm.OldestCompleted.Bucket().Start) {
		r := ref
		cm.OldestCompleted = &r
	}
	if cm.NewestCompleted == nil || ref.Bucket().Start.After(cm.NewestCompleted.Bucket().Start) {
		r := ref
		cm.NewestCompleted = &r
	}
}

func setCollectionActive(cm *CollectionMeta, collBase string, hb HourBucket, hourMetaKey string, status HourStatus) {
	ref := HourRefFromBucket(hb, HourMetaRefRelative(hb), status)
	cm.Active = &ref
}

func persistShutdown(meta *MetaStore, log *zap.Logger, ns NSSpec, collBase string, collMeta *CollectionMeta, hourBase string, hm *HourMeta, hb HourBucket, coord *shutdown.Coordinator) error {
	if hm.Status == HourStatusComplete {
		if coord != nil {
			coord.NotifyPersisted()
		}
		return context.Canceled
	}
	hm.Status = HourStatusPartial
	setCollectionActive(collMeta, collBase, hb, HourMetaKey(hourBase), HourStatusPartial)
	persistCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := meta.SaveHourMeta(persistCtx, hourBase, hm); err != nil {
		log.Error("save hour meta on shutdown", zap.Error(err))
	} else if err := meta.SaveCollectionMeta(persistCtx, collBase, collMeta); err != nil {
		log.Error("save collection meta on shutdown", zap.Error(err))
	}
	if coord != nil {
		coord.NotifyPersisted()
	}
	log.Warn("shutdown requested, checkpoint saved (hour remains partial; next run will wipe and re-backup)",
		zap.String("collection", ns.String()),
		zap.String("hour", hb.String()),
		zap.Bool("forced", coord != nil && coord.Forced()),
		zap.Int("uploaded", countUploaded(hm.Tenants)),
		zap.Int("tenants", len(hm.Tenants)),
	)
	return context.Canceled
}
