package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/logeable/mongomig/internal/config"
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
	ResetHour      *HourBucket
	RemotePrefix   string
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
	if opts.ResetHour != nil {
		log.Debug("hourly sync reset_hour", zap.String("reset_hour", opts.ResetHour.String()))
	}

	meta, err := NewMetaStore(remote, cfg.StagingDir)
	if err != nil {
		return err
	}
	archiver := NewTenantHourArchive(cfg)
	now := time.Now().UTC()
	log.Debug("sync clock", zap.Time("now_utc", now))

	for _, ns := range opts.Collections {
		if err := ctx.Err(); err != nil {
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
		log.Debug("collection meta loaded",
			zap.Bool("found", collFound),
			zap.String("meta_key", CollectionMetaKey(collBase)),
		)
		logCollectionMeta(log, collMeta)

		if opts.ResetHour != nil {
			hb := *opts.ResetHour
			hourBase := HourBase(collBase, hb)
			log.Warn("reset-hour: deleting hour prefix", zap.String("hour", hb.String()))
			if err := remote.DeletePrefix(ctx, hourBase+"/"); err != nil {
				return err
			}
		}

		// Resume active in_progress hour before scanning forward.
		if collMeta.Active != nil {
			activeBucket := collMeta.Active.Bucket()
			hourBase := HourBase(collBase, activeBucket)
			hm, ok, err := meta.LoadHourMeta(ctx, hourBase)
			if err != nil {
				return err
			}
			if ok && hm.Status == HourStatusInProgress {
				log.Info("resume in_progress hour", zap.String("hour", activeBucket.String()))
				log.Debug("active hour meta",
					zap.String("status", string(hm.Status)),
					zap.Int("tenant_rows", len(hm.Tenants)),
					zap.Int("uploaded", countUploaded(hm.Tenants)),
				)
				if err := processHour(ctx, cfg, remote, meta, archiver, log, opts, ns, collBase, collMeta, activeBucket, hm, now, true); err != nil {
					return err
				}
				if err := meta.SaveCollectionMeta(ctx, collBase, collMeta); err != nil {
					return err
				}
			}
		}

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
			if err := ctx.Err(); err != nil {
				return err
			}
			if collMeta.Active != nil && sameHour(collMeta.Active.Bucket(), hb) && collMeta.Active.Status != HourStatusComplete {
				log.Debug("skip hour already resumed via active", zap.String("hour", hb.String()))
				continue // already handled resume
			}
			hourBase := HourBase(collBase, hb)
			log.Debug("processing hour", zap.String("hour", hb.String()), zap.String("hour_base", hourBase))
			hm, exists, err := meta.LoadHourMeta(ctx, hourBase)
			if err != nil {
				return err
			}
			if exists {
				log.Debug("hour meta loaded",
					zap.String("hour", hb.String()),
					zap.String("status", string(hm.Status)),
					zap.Int("tenants", len(hm.Tenants)),
					zap.Int("uploaded", countUploaded(hm.Tenants)),
				)
			} else {
				log.Debug("hour meta missing, will create", zap.String("hour", hb.String()))
			}
			if exists && hm.Status == HourStatusComplete && !opts.ForceHour {
				log.Debug("skip complete hour", zap.String("hour", hb.String()))
				continue
			}
			wipePartial := exists && hm.Status == HourStatusPartial
			if wipePartial {
				log.Info("partial hour: wipe and rediscover", zap.String("hour", hb.String()))
				if err := remote.DeletePrefix(ctx, hourBase+"/"); err != nil {
					return err
				}
				hm = nil
				exists = false
			}
			if err := processHour(ctx, cfg, remote, meta, archiver, log, opts, ns, collBase, collMeta, hb, hm, now, exists); err != nil {
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
	existing *HourMeta,
	now time.Time,
	resume bool,
) error {
	hourBase := HourBase(collBase, hb)
	hourMetaKey := HourMetaKey(hourBase)

	var hm *HourMeta
	if resume && existing != nil {
		hm = existing
		log.Debug("hour resume existing tenant list",
			zap.String("hour", hb.String()),
			zap.Int("tenants", len(hm.Tenants)),
			zap.Int("pending", len(hm.Tenants)-countUploaded(hm.Tenants)),
		)
	} else {
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
		log.Debug("discover tenants done",
			zap.String("hour", hb.String()),
			zap.Int("count", len(tenants)),
		)
		hm = newHourMeta(ns, hb, HourStatusInProgress)
		for _, tk := range tenants {
			row, err := tenantRowForKey(hourBase, tk)
			if err != nil {
				log.Debug("tenant shard path skipped",
					zap.String("tenant_key", tk),
					zap.Error(err),
				)
				hm.Tenants = append(hm.Tenants, TenantMetaRow{TenantKey: tk, Error: err.Error()})
				continue
			}
			hm.Tenants = append(hm.Tenants, row)
		}
		log.Debug("hour meta initial save", zap.String("hour_meta_key", hourMetaKey), zap.Int("tenant_rows", len(hm.Tenants)))
		if err := meta.SaveHourMeta(ctx, hourBase, hm); err != nil {
			return err
		}
		setCollectionActive(collMeta, collBase, hb, hourMetaKey, HourStatusInProgress)
		if err := meta.SaveCollectionMeta(ctx, collBase, collMeta); err != nil {
			return err
		}
	}

	for i := range hm.Tenants {
		if err := ctx.Err(); err != nil {
			return persistShutdown(ctx, meta, log, ns, collBase, collMeta, hourBase, hm, hb, now)
		}
		row := &hm.Tenants[i]
		if row.Uploaded && row.Error == "" {
			log.Debug("skip tenant already uploaded",
				zap.String("hour", hb.String()),
				zap.String("tenant", row.TenantKey),
				zap.String("object_key", row.ObjectKey),
			)
			continue
		}
		if row.Error != "" && row.Uploaded {
			continue
		}
		if err := backupOneTenant(ctx, cfg, remote, meta, archiver, log, opts, ns, collBase, hourBase, hb, hm, row, i); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return persistShutdown(ctx, meta, log, ns, collBase, collMeta, hourBase, hm, hb, now)
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

	if err := ctx.Err(); err != nil {
		return persistShutdown(ctx, meta, log, ns, collBase, collMeta, hourBase, hm, hb, now)
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
	obj := TenantObjectKey(hourBase, rel)
	return TenantMetaRow{
		TenantKey:   tenantKey,
		Shard:       shard,
		DataRelPath: rel,
		ObjectKey:   obj,
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
) error {
	if row.TenantKey == "" {
		return fmt.Errorf("empty tenant_key at index %d", idx)
	}
	shard, err := TenantShard(row.TenantKey)
	if err != nil {
		return err
	}
	rel := TenantDataRelPath(shard, row.TenantKey)
	obj := TenantObjectKey(hourBase, rel)
	row.Shard = shard
	row.DataRelPath = rel
	row.ObjectKey = obj
	row.Error = ""

	workDir := cfg.AbsStaging("hourly", ns.DB, ns.Coll, hb.String(), row.TenantKey)
	log.Debug("tenant backup start",
		zap.String("collection", ns.String()),
		zap.String("hour", hb.String()),
		zap.String("tenant", row.TenantKey),
		zap.String("shard", shard),
		zap.String("work_dir", workDir),
		zap.String("object_key", obj),
	)
	tarPath, sha, size, err := archiver.DumpAndTar(ctx, log, ns, opts.TenantField, row.TenantKey, opts.TenantNumeric, opts.TimeField, hb.Start, hb.End, workDir)
	if err != nil {
		return err
	}
	log.Debug("uploading dump.tar", zap.String("local", tarPath), zap.Int64("size_bytes", size))
	if err := remote.UploadFile(ctx, tarPath, obj); err != nil {
		return err
	}
	row.Uploaded = true
	row.SHA256 = sha
	row.SizeBytes = size
	log.Info("tenant uploaded",
		zap.String("tenant", row.TenantKey),
		zap.String("object_key", obj),
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

func persistShutdown(ctx context.Context, meta *MetaStore, log *zap.Logger, ns NSSpec, collBase string, collMeta *CollectionMeta, hourBase string, hm *HourMeta, hb HourBucket, _ time.Time) error {
	if hm.Status == HourStatusComplete {
		return context.Canceled
	}
	if hm.Status == HourStatusInProgress {
		hm.Status = HourStatusPartial
	}
	setCollectionActive(collMeta, collBase, hb, HourMetaKey(hourBase), hm.Status)
	_ = meta.SaveHourMeta(ctx, hourBase, hm)
	_ = meta.SaveCollectionMeta(ctx, collBase, collMeta)
	log.Warn("shutdown requested, resumable checkpoint saved",
		zap.String("collection", ns.String()),
		zap.String("hour", hb.String()),
		zap.String("hour_status", string(hm.Status)),
		zap.Int("uploaded", countUploaded(hm.Tenants)),
		zap.Int("tenants", len(hm.Tenants)),
	)
	return context.Canceled
}
