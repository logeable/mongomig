package main

import (
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/logeable/mongomig/internal/backup"
	"github.com/logeable/mongomig/internal/config"
	mongolog "github.com/logeable/mongomig/internal/log"
	"github.com/logeable/mongomig/internal/shutdown"
	"github.com/logeable/mongomig/internal/storage"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newRestoreCmd(v *viper.Viper) *cobra.Command {
	var (
		dbName          string
		collections     string
		tenantField     string
		timeField       string
		tenantNumeric   bool
		fromHour        string
		toHour          string
		drop            bool
		dryRun          bool
		resetCheckpoint bool
		shutdownGrace   time.Duration
	)
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore UTC hourly tenant backups from OSS into MongoDB",
		Long: `从 OSS 按 UTC 小时顺序恢复各租户 dump.tar 到 mongo_uri 指向的库。

用户场景（默认行为）:
  1. 首次灌库: restore --db <db> --collections <c> --drop
  2. 日常增量: restore --db <db> --collections <c>  （从 checkpoint 续到 OSS 最新 complete 小时）
  3. 中断续跑: 同上，未写完 checkpoint 的整小时会重跑
  4. 换库/重灌: restore --db <db> --drop --reset-checkpoint

恢复 complete 与 partial（含 active 小时）桶内已上传租户；partial 不推进 checkpoint。
进度写入 {db}._mongomig_restore（见 mongomig.yaml restore_checkpoint_collection）。
每个租户 restore 前会按与 backup 相同的 tenant/time 窗口 deleteMany；每个集合从 OSS indexes.json 创建索引（须先 backup 生成该文件）。

显式 --from-hour/--to-hour 覆盖自动范围；--dry-run 只打印计划。
Ctrl+C：首次信号结束新租户/新小时，当前租户尽量跑完；不推进 checkpoint（未写完的整小时下次重跑）。再次或超时强制取消。`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.LoadViper(v)
			if err != nil {
				return err
			}
			if err := cfg.Validate(); err != nil {
				return err
			}
			if err := cfg.EnsureStaging(); err != nil {
				return err
			}
			if strings.TrimSpace(dbName) == "" {
				dbName = strings.TrimSpace(v.GetString("db"))
			}
			if strings.TrimSpace(dbName) == "" {
				dbName = strings.TrimSpace(cfg.DB)
			}
			if strings.TrimSpace(dbName) == "" {
				return fmt.Errorf("--db is required (or set db in mongomig.yaml)")
			}

			logger, err := mongolog.New(cfg.LogLevel)
			if err != nil {
				return err
			}
			defer func() { _ = logger.Sync() }()

			coord := shutdown.New(shutdownGrace)
			defer coord.Stop()
			ctx := coord.Context()

			specs, err := backup.ResolveCollectionSpecs(ctx, logger, cfg.MongoURI, dbName, collections, cfg.RestoreCheckpointColl())
			if err != nil {
				return err
			}

			var from *backup.HourBucket
			if strings.TrimSpace(fromHour) != "" {
				fb, err := backup.ParseHourFlag(fromHour)
				if err != nil {
					return err
				}
				from = &fb
			}
			var to *backup.HourBucket
			if strings.TrimSpace(toHour) != "" {
				tb, err := backup.ParseHourFlag(toHour)
				if err != nil {
					return err
				}
				to = &tb
			}

			remote, err := storage.NewRemote(ctx, cfg)
			if err != nil {
				return err
			}
			opts := backup.HourlyRestoreOpts{
				Collections:          specs,
				TenantField:          tenantField,
				TimeField:            timeField,
				TenantNumeric:        tenantNumeric,
				FromHour:             from,
				ToHour:               to,
				RemotePrefix:         cfg.RemotePrefix,
				CheckpointCollection: cfg.RestoreCheckpointCollection,
				Drop:                 drop,
				DryRun:               dryRun,
				ResetCheckpoint:      resetCheckpoint,
				Shutdown:             coord,
			}
			if err := backup.RunHourlyOSSRestore(ctx, cfg, remote, logger, opts); err != nil {
				if coord.Stopping() {
					logger.Warn("restore stopped after shutdown signal", zap.Error(err), zap.Bool("forced", coord.Forced()))
				} else {
					logger.Warn("restore stopped", zap.Error(err))
				}
				return err
			}
			logger.Info("restore finished")
			return nil
		},
	}
	cmd.Flags().StringVar(&dbName, "db", "", "Target MongoDB database (required; or mongomig.yaml db)")
	cmd.Flags().StringVar(&collections, "collections", "", "Comma-separated collection names; default: all non-system collections in --db")
	cmd.Flags().StringVar(&tenantField, "tenant-field", "tenant_key", "BSON tenant field (must match backup)")
	cmd.Flags().StringVar(&timeField, "time-field", "created_at", "BSON time field for hour window (must match backup)")
	cmd.Flags().BoolVar(&tenantNumeric, "tenant-key-numeric", false, "Tenant id is numeric in queries (must match backup)")
	cmd.Flags().StringVar(&fromHour, "from-hour", "", "First UTC hour YYYY-MM-DDTHH (default: checkpoint+1 or OSS oldest_completed)")
	cmd.Flags().StringVar(&toHour, "to-hour", "", "Last UTC hour inclusive (default: max(OSS newest_completed, active))")
	cmd.Flags().BoolVar(&drop, "drop", false, "Drop target collection before first mongorestore (empty cluster / full reload)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print restore plan only")
	cmd.Flags().BoolVar(&resetCheckpoint, "reset-checkpoint", false, "Clear MongoDB restore progress then restore from OSS oldest")
	cmd.Flags().DurationVar(&shutdownGrace, "shutdown-grace", 30*time.Second, "After first Ctrl+C, wait up to this long for current tenant restore to finish")
	_ = v.BindPFlag("db", cmd.Flags().Lookup("db"))
	return cmd
}
