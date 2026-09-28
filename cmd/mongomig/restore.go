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
		tenantKeys      []string
		tenantFile      string
		tenantField     string
		timeField       string
		tenantNumeric   bool
		fromHour        string
		toHour          string
		drop            bool
		dryRun          bool
		noCheckpoint    bool
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
  5. 指定租户: restore --db <db> --tenant-key <key> [--tenant-key <key> ...]

恢复 complete 与 partial（含 active 小时）桶内已上传租户；partial 不推进 checkpoint。
进度写入 {db}._mongomig_restore（见 mongomig.yaml restore_checkpoint_collection）。
每个租户 restore 前会按与 backup 相同的 tenant/time 窗口 deleteMany；全量模式每个集合从 OSS indexes.json 同步索引（须先 backup 生成该文件）。
指定租户模式使用每个 collection + tenant_key 独立的 checkpoint，禁止 --drop，避免影响其他租户。

显式 --from-hour/--to-hour 覆盖自动范围；--dry-run 只打印计划。
--tenant-file 每行一个 tenant_key，可与重复的 --tenant-key 合并；--no-checkpoint 只用于指定时间范围的一次性租户恢复。
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
			resolvedTenantKeys, err := backup.ResolveRestoreTenantKeys(tenantKeys, tenantFile)
			if err != nil {
				return err
			}
			if len(resolvedTenantKeys) > 0 && drop {
				return fmt.Errorf("--drop cannot be used with --tenant-key/--tenant-file")
			}
			if noCheckpoint && len(resolvedTenantKeys) == 0 {
				return fmt.Errorf("--no-checkpoint requires --tenant-key or --tenant-file")
			}
			if noCheckpoint && resetCheckpoint {
				return fmt.Errorf("--no-checkpoint cannot be combined with --reset-checkpoint")
			}

			logger, err := mongolog.New(cfg.LogLevel)
			if err != nil {
				return err
			}
			defer func() { _ = logger.Sync() }()

			coord := shutdown.New(shutdownGrace)
			defer coord.Stop()
			ctx := coord.Context()

			remote, err := storage.NewRemote(ctx, cfg)
			if err != nil {
				return err
			}
			specs, err := backup.ResolveRestoreCollectionSpecs(ctx, logger, remote, cfg.RemotePrefix, dbName, collections, cfg.RestoreCheckpointColl())
			if err != nil {
				return err
			}
			names := make([]string, len(specs))
			for i, s := range specs {
				names[i] = s.String()
			}
			logger.Info("restore collections resolved from OSS",
				zap.String("db", dbName),
				zap.Strings("collections", names),
			)

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
			opts := backup.HourlyRestoreOpts{
				Collections:          specs,
				TenantKeys:           resolvedTenantKeys,
				TenantField:          tenantField,
				TimeField:            timeField,
				TenantNumeric:        tenantNumeric,
				FromHour:             from,
				ToHour:               to,
				RemotePrefix:         cfg.RemotePrefix,
				CheckpointCollection: cfg.RestoreCheckpointCollection,
				Drop:                 drop,
				DryRun:               dryRun,
				NoCheckpoint:         noCheckpoint,
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
	cmd.Flags().StringVar(&collections, "collections", "", "Comma-separated collection names; default: discover collections with meta.json on OSS under remote_prefix/--db")
	cmd.Flags().StringArrayVar(&tenantKeys, "tenant-key", nil, "Exact 42-character tenant_key to restore; repeat for multiple tenants")
	cmd.Flags().StringVar(&tenantFile, "tenant-file", "", "Text file with one exact 42-character tenant_key per line")
	cmd.Flags().StringVar(&tenantField, "tenant-field", "tenant_key", "BSON tenant field (must match backup)")
	cmd.Flags().StringVar(&timeField, "time-field", "created_at", "BSON time field for hour window (must match backup)")
	cmd.Flags().BoolVar(&tenantNumeric, "tenant-key-numeric", false, "Tenant id is numeric in queries (must match backup)")
	cmd.Flags().StringVar(&fromHour, "from-hour", "", "First UTC hour YYYY-MM-DDTHH (default: checkpoint+1 or OSS oldest_completed)")
	cmd.Flags().StringVar(&toHour, "to-hour", "", "Last UTC hour inclusive (default: max(OSS newest_completed, active))")
	cmd.Flags().BoolVar(&drop, "drop", false, "Drop target collection before first mongorestore (empty cluster / full reload)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print restore plan only")
	cmd.Flags().BoolVar(&noCheckpoint, "no-checkpoint", false, "Tenant restore only: do not read or write tenant checkpoint; requires --from-hour and --to-hour")
	cmd.Flags().BoolVar(&resetCheckpoint, "reset-checkpoint", false, "Clear MongoDB restore progress then restore from OSS oldest")
	cmd.Flags().DurationVar(&shutdownGrace, "shutdown-grace", 30*time.Second, "After first Ctrl+C, wait up to this long for current tenant restore to finish")
	_ = v.BindPFlag("db", cmd.Flags().Lookup("db"))
	return cmd
}
