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
	meta, err := NewMetaStore(remote, cfg.StagingDir)
	if err != nil {
		return err
	}
	archiver := NewTenantHourArchive(cfg)
	now := time.Now().UTC()

	for _, ns := range opts.Collections {
		if err := ctx.Err(); err != nil {
			return err
		}
		collBase := CollectionBase(opts.RemotePrefix, ns.DB, ns.Coll)
		log.Info("collection backup start", zap.String("collection", ns.String()), zap.String("base", collBase))

		if err := WriteCollectionIndexesJSON(ctx, cfg.MongoURI, remote, meta, collBase, ns); err != nil {
			return fmt.Errorf("%s indexes: %w", ns.String(), err)
		}

		collMeta, _, err := meta.LoadCollectionMeta(ctx, collBase)
		if err != nil {
			return err
		}
		if collMeta == nil {
			collMeta = &CollectionMeta{DB: ns.DB, Collection: ns.Coll}
		}
		collMeta.DB = ns.DB
		collMeta.Collection = ns.Coll

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
		endHour := opts.ToHour
		if opts.FromHour != nil {
			startHour = *opts.FromHour
		}
		for _, hb := range HoursInclusive(startHour, endHour) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if collMeta.Active != nil && sameHour(collMeta.Active.Bucket(), hb) && collMeta.Active.Status != HourStatusComplete {
				continue // already handled resume
			}
			hourBase := HourBase(collBase, hb)
			hm, exists, err := meta.LoadHourMeta(ctx, hourBase)
			if err != nil {
				return err
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
	}
	return nil
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
	} else {
		if opts.DryRun {
			log.Info("dry-run: would backup hour", zap.String("hour", hb.String()))
			return nil
		}
		tenants, err := DiscoverTenantsInHour(ctx, cfg.MongoURI, ns, opts.TenantField, opts.TimeField, opts.TenantNumeric, hb.Start, hb.End)
		if err != nil {
			return err
		}
		hm = newHourMeta(ns, hb, HourStatusInProgress)
		for _, tk := range tenants {
			row, err := tenantRowForKey(hourBase, tk)
			if err != nil {
				hm.Tenants = append(hm.Tenants, TenantMetaRow{TenantKey: tk, Error: err.Error()})
				continue
			}
			hm.Tenants = append(hm.Tenants, row)
		}
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
			return persistShutdown(ctx, meta, remote, collBase, collMeta, hourBase, hm, hb, now)
		}
		row := &hm.Tenants[i]
		if row.Uploaded && row.Error == "" {
			continue
		}
		if row.Error != "" && row.Uploaded {
			continue
		}
		if err := backupOneTenant(ctx, cfg, remote, meta, archiver, log, opts, ns, collBase, hourBase, hb, hm, row, i); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return persistShutdown(ctx, meta, remote, collBase, collMeta, hourBase, hm, hb, now)
			}
			row.Error = err.Error()
			row.Uploaded = false
			_ = meta.SaveHourMeta(ctx, hourBase, hm)
			log.Error("tenant backup failed", zap.String("tenant", row.TenantKey), zap.Error(err))
			continue
		}
		if err := meta.SaveHourMeta(ctx, hourBase, hm); err != nil {
			return err
		}
	}

	if err := ctx.Err(); err != nil {
		return persistShutdown(ctx, meta, remote, collBase, collMeta, hourBase, hm, hb, now)
	}

	return finalizeHour(ctx, meta, collBase, collMeta, hourBase, hb, hm, now)
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
	tarPath, sha, size, err := archiver.DumpAndTar(ctx, ns, opts.TenantField, row.TenantKey, opts.TenantNumeric, opts.TimeField, hb.Start, hb.End, workDir)
	if err != nil {
		return err
	}
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
		_ = os.RemoveAll(workDir)
	}
	return nil
}

func finalizeHour(ctx context.Context, meta *MetaStore, collBase string, collMeta *CollectionMeta, hourBase string, hb HourBucket, hm *HourMeta, now time.Time) error {
	allOK := true
	for _, t := range hm.Tenants {
		if t.Error != "" || !t.Uploaded {
			allOK = false
			break
		}
	}
	hourEnded := !now.Before(hb.End)
	if hourEnded && allOK {
		hm.Status = HourStatusComplete
		hm.CompletedAt = now.UTC().Format(time.RFC3339Nano)
		if err := meta.SaveHourMeta(ctx, hourBase, hm); err != nil {
			return err
		}
		ref := HourRefFromBucket(hb, HourMetaRefRelative(hb), HourStatusComplete)
		updateCompletedBounds(collMeta, ref)
		collMeta.Active = nil
		return meta.SaveCollectionMeta(ctx, collBase, collMeta)
	}
	hm.Status = HourStatusPartial
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

func persistShutdown(ctx context.Context, meta *MetaStore, _ storage.Backend, collBase string, collMeta *CollectionMeta, hourBase string, hm *HourMeta, hb HourBucket, _ time.Time) error {
	if hm.Status == HourStatusComplete {
		return context.Canceled
	}
	if hm.Status == HourStatusInProgress {
		hm.Status = HourStatusPartial
	}
	setCollectionActive(collMeta, collBase, hb, HourMetaKey(hourBase), hm.Status)
	_ = meta.SaveHourMeta(ctx, hourBase, hm)
	_ = meta.SaveCollectionMeta(ctx, collBase, collMeta)
	return context.Canceled
}
