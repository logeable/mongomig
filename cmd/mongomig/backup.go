package main

import (
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/logeable/mongomig/internal/backup"
	"github.com/logeable/mongomig/internal/config"
	mongolog "github.com/logeable/mongomig/internal/log"
	"github.com/logeable/mongomig/internal/storage"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newBackupCmd(v *viper.Viper) *cobra.Command {
	var (
		dbName        string
		collections   string
		tenantField   string
		timeField     string
		tenantNumeric bool
		fromHour      string
		toHour        string
		cleanupLocal  bool
		dryRun        bool
		forceHour     bool
		resetHour     string
	)
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "UTC hourly collection backup with tenant discover to OSS",
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

			logger, err := mongolog.New(cfg.LogLevel)
			if err != nil {
				return err
			}
			defer func() { _ = logger.Sync() }()

			ctx, cancel := notifyContext()
			defer cancel()

			specs, err := backup.ResolveCollectionSpecs(ctx, cfg.MongoURI, dbName, collections)
			if err != nil {
				return err
			}
			names := make([]string, len(specs))
			for i, s := range specs {
				names[i] = s.String()
			}
			logger.Info("backup collections resolved",
				zap.String("db", dbName),
				zap.Strings("collections", names),
			)

			to := backup.HourBucketUTC(time.Now().UTC())
			if strings.TrimSpace(toHour) != "" {
				to, err = backup.ParseHourFlag(toHour)
				if err != nil {
					return err
				}
			}
			var from *backup.HourBucket
			if strings.TrimSpace(fromHour) != "" {
				fb, err := backup.ParseHourFlag(fromHour)
				if err != nil {
					return err
				}
				from = &fb
			}
			var reset *backup.HourBucket
			if strings.TrimSpace(resetHour) != "" {
				rh, err := backup.ParseHourFlag(resetHour)
				if err != nil {
					return err
				}
				reset = &rh
			}

			remote, err := storage.NewRemote(ctx, cfg)
			if err != nil {
				return err
			}
			opts := backup.HourlySyncOpts{
				Collections:   specs,
				TenantField:   tenantField,
				TimeField:     timeField,
				TenantNumeric: tenantNumeric,
				FromHour:      from,
				ToHour:        to,
				CleanupLocal:  cleanupLocal,
				DryRun:        dryRun,
				ForceHour:     forceHour,
				ResetHour:     reset,
				RemotePrefix:  cfg.RemotePrefix,
			}
			err = backup.RunHourlyOSSSync(ctx, cfg, remote, logger, opts)
			if err != nil {
				logger.Warn("backup stopped", zap.Error(err))
				return err
			}
			logger.Info("backup finished")
			return nil
		},
	}
	cmd.Flags().StringVar(&dbName, "db", "", "MongoDB database name (required; or MONGOMIG_DB)")
	cmd.Flags().StringVar(&collections, "collections", "", "Optional comma-separated collection names under --db; default: discover all collections")
	cmd.Flags().StringVar(&tenantField, "tenant-field", "tenant_key", "BSON tenant field")
	cmd.Flags().StringVar(&timeField, "time-field", "created_at", "BSON time field for hour window")
	cmd.Flags().BoolVar(&tenantNumeric, "tenant-key-numeric", false, "Tenant id is numeric JSON in queries")
	cmd.Flags().StringVar(&fromHour, "from-hour", "", "First UTC hour YYYY-MM-DDTHH")
	cmd.Flags().StringVar(&toHour, "to-hour", "", "Last UTC hour inclusive (default: current UTC hour)")
	cmd.Flags().BoolVar(&cleanupLocal, "cleanup-local", false, "Remove local staging per tenant after upload")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Plan only; no dump/upload")
	cmd.Flags().BoolVar(&forceHour, "force-hour", false, "Re-backup hours already marked complete")
	cmd.Flags().StringVar(&resetHour, "reset-hour", "", "Delete OSS prefix for one UTC hour before backup")
	_ = v.BindPFlag("db", cmd.Flags().Lookup("db"))
	return cmd
}
