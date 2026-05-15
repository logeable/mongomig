package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/logeable/mongomig/internal/backup"
	"github.com/logeable/mongomig/internal/config"
	"github.com/logeable/mongomig/internal/storage"
	"github.com/spf13/cobra"
)

var (
	mongoURI        string
	stagingDir      string
	chain           string
	gzipDump        bool
	parallelCols    int
	dumpTimeout     time.Duration
	restoreTimeout  time.Duration
	mongodumpBin    string
	mongorestoreBin string
	remotePrefix    string

	s3Endpoint  string
	s3Region    string
	s3Bucket    string
	s3PathStyle bool
	s3AccessKey string
	s3SecretKey string

	restoreForce bool
	restoreDrop  bool

	tenantBackupKey           string
	tenantBackupDate          string
	tenantCreatedAt           string
	tenantBackupCollections   string
	tenantBackupField         string
	tenantBackupNumeric       bool
	tenantCleanupLocal        bool
	ossExportCreatedAt        string

	ossExportDaysTZ          string
	ossExportDaysTimeField   string
	ossExportDaysFrom        string
	ossExportDaysTo          string
	ossSyncJobID             string
	ossExportDaysDryRun      bool
	ossExportDaysRedoFailed  bool
	ossExportDaysNoSkipOK    bool

	restoreTenantKey    string
	restoreTenantDate   string
	restoreCreatedAt    string
	restoreTenantRun    string
)

func main() {
	if err := newRoot().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:   "mongomig",
		Short: "Tenant-scoped MongoDB export/import on S3-compatible OSS (data + indexes)",
	}
	root.PersistentFlags().StringVar(&mongoURI, "mongo-uri", "", "MongoDB connection URI (or MONGOMIG_MONGO_URI)")
	root.PersistentFlags().StringVar(&stagingDir, "staging-dir", "", "Local staging directory for dumps")
	root.PersistentFlags().StringVar(&chain, "chain", "default", "Logical backup chain name (legacy full/oplog restore)")
	root.PersistentFlags().BoolVar(&gzipDump, "gzip", true, "Pass --gzip to mongodump/mongorestore")
	root.PersistentFlags().IntVar(&parallelCols, "parallel-collections", 4, "mongodump --numParallelCollections (0=omit, legacy full only)")
	root.PersistentFlags().DurationVar(&dumpTimeout, "dump-timeout", 0, "Per-dump timeout (0=24h)")
	root.PersistentFlags().DurationVar(&restoreTimeout, "restore-timeout", 0, "Per-restore timeout (0=24h)")
	root.PersistentFlags().StringVar(&mongodumpBin, "mongodump-path", "", "Path to mongodump binary (default: PATH)")
	root.PersistentFlags().StringVar(&mongorestoreBin, "mongorestore-path", "", "Path to mongorestore binary (default: PATH)")
	root.PersistentFlags().StringVar(&remotePrefix, "remote-prefix", "mongomig", "Key prefix on object storage")

	root.PersistentFlags().StringVar(&s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint (empty disables upload/download)")
	root.PersistentFlags().StringVar(&s3Region, "s3-region", "us-east-1", "Region for signing")
	root.PersistentFlags().StringVar(&s3Bucket, "s3-bucket", "", "Bucket name")
	root.PersistentFlags().BoolVar(&s3PathStyle, "s3-path-style", true, "Use path-style addressing (typical for MinIO)")
	root.PersistentFlags().StringVar(&s3AccessKey, "s3-access-key", "", "Access key (or MONGOMIG_S3_ACCESS_KEY_ID)")
	root.PersistentFlags().StringVar(&s3SecretKey, "s3-secret-key", "", "Secret key (or MONGOMIG_S3_SECRET_ACCESS_KEY)")

	ossCmd := &cobra.Command{
		Use:   "oss",
		Short: "Export or import tenant data on OSS with tenant_key + created_at path layout",
	}
	ossExport := &cobra.Command{
		Use:   "export",
		Short: "Dump tenant-filtered collections + index metadata, upload to OSS",
		RunE:  runOSSExport,
	}
	ossExport.Flags().StringVar(&tenantBackupKey, "tenant-key", "", "Tenant id (BSON field tenant_key by default)")
	ossExport.Flags().StringVar(&ossExportCreatedAt, "created-at", "", "Partition time: YYYY-MM-DD or RFC3339 (UTC path segment)")
	ossExport.Flags().StringVar(&tenantBackupCollections, "collections", "", "Comma-separated db.collection; multiple supported (e.g. revol.a,revol.b)")
	ossExport.Flags().StringVar(&tenantBackupField, "tenant-field", "tenant_key", "BSON field for tenant filter")
	ossExport.Flags().BoolVar(&tenantBackupNumeric, "tenant-key-numeric", false, "Tenant id is JSON number in query")
	ossExport.Flags().BoolVar(&tenantCleanupLocal, "cleanup-local", false, "Remove local dump dir after successful upload")
	_ = ossExport.MarkFlagRequired("tenant-key")
	_ = ossExport.MarkFlagRequired("created-at")
	_ = ossExport.MarkFlagRequired("collections")

	ossExportDays := &cobra.Command{
		Use:   "export-days",
		Short: "Day-by-day tenant export from earliest data to today (civil calendar in timezone); progress JSON on OSS",
		Long: "Runs one mongodump+upload per civil day with tenant_key AND time-field range [day start, next day).\n" +
			"Progress and metadata are stored at: {remote-prefix}/{tenant}/_meta/day-sync-{sync-job-id}.json",
		RunE: runOSSExportDays,
	}
	ossExportDays.Flags().StringVar(&tenantBackupKey, "tenant-key", "", "Tenant id (BSON field tenant_key by default)")
	ossExportDays.Flags().StringVar(&tenantBackupCollections, "collections", "", "Comma-separated db.collection; multiple supported (e.g. revol.a,revol.b)")
	ossExportDays.Flags().StringVar(&tenantBackupField, "tenant-field", "tenant_key", "BSON field for tenant filter")
	ossExportDays.Flags().BoolVar(&tenantBackupNumeric, "tenant-key-numeric", false, "Tenant id is JSON number in query")
	ossExportDays.Flags().StringVar(&ossExportDaysTZ, "timezone", "Asia/Shanghai", "IANA timezone for civil calendar days (e.g. Asia/Shanghai)")
	ossExportDays.Flags().StringVar(&ossExportDaysTimeField, "time-field", "created_at", "BSON date field for per-day range filter")
	ossExportDays.Flags().StringVar(&ossExportDaysFrom, "from-day", "", "First civil day YYYY-MM-DD in timezone (default: earliest matching doc)")
	ossExportDays.Flags().StringVar(&ossExportDaysTo, "to-day", "", "Last civil day inclusive YYYY-MM-DD (default: min(latest data, today in timezone))")
	ossExportDays.Flags().StringVar(&ossSyncJobID, "sync-job-id", "default", "Progress object key segment under tenant/_meta/")
	ossExportDays.Flags().BoolVar(&tenantCleanupLocal, "cleanup-local", false, "Remove local dump dir after each successful day upload")
	ossExportDays.Flags().BoolVar(&ossExportDaysDryRun, "dry-run", false, "List planned days only; no dump/upload")
	ossExportDays.Flags().BoolVar(&ossExportDaysRedoFailed, "redo-failed", false, "Only retry days previously marked failed in OSS progress")
	ossExportDays.Flags().BoolVar(&ossExportDaysNoSkipOK, "no-skip-ok", false, "Re-export days already marked ok (default skips ok for resume)")
	_ = ossExportDays.MarkFlagRequired("tenant-key")
	_ = ossExportDays.MarkFlagRequired("collections")

	ossImport := &cobra.Command{
		Use:   "import",
		Short: "Download tenant snapshot from OSS and mongorestore (data + indexes from metadata)",
		RunE:  runOSSImport,
	}
	ossImport.Flags().BoolVar(&restoreForce, "force", false, "Required to confirm import to live URI")
	ossImport.Flags().BoolVar(&restoreDrop, "drop", false, "Pass --drop to mongorestore")
	ossImport.Flags().StringVar(&restoreTenantKey, "tenant-key", "", "Tenant id (same as export)")
	ossImport.Flags().StringVar(&restoreCreatedAt, "created-at", "", "Same partition as export (YYYY-MM-DD or RFC3339)")
	ossImport.Flags().StringVar(&restoreTenantRun, "run-id", "", "Backup run folder; default latest under partition")
	_ = ossImport.MarkFlagRequired("tenant-key")
	_ = ossImport.MarkFlagRequired("created-at")

	backupCmd := &cobra.Command{Use: "backup", Short: "Legacy: full instance or oplog incremental (mongodump)"}
	backupFull := &cobra.Command{
		Use:   "full",
		Short: "Full instance dump with --oplog",
		RunE:  runBackupFull,
	}
	backupIncr := &cobra.Command{
		Use:     "incremental",
		Aliases: []string{"incr"},
		Short:   "Dump oplog.rs after last successful backup",
		RunE:    runBackupIncremental,
	}
	backupTenant := &cobra.Command{
		Use:   "tenant",
		Short: "Same as `oss export` (compat); prefer oss export",
		RunE:  runBackupTenant,
	}
	backupTenant.Flags().StringVar(&tenantBackupKey, "tenant-key", "", "Tenant id")
	backupTenant.Flags().StringVar(&tenantBackupDate, "tenant-date", "", "YYYY-MM-DD only (UTC midnight partition); use --created-at for full time")
	backupTenant.Flags().StringVar(&tenantCreatedAt, "created-at", "", "Partition: YYYY-MM-DD or RFC3339 (overrides --tenant-date if both set)")
	backupTenant.Flags().StringVar(&tenantBackupCollections, "tenant-collections", "", "Comma-separated db.collection (required)")
	backupTenant.Flags().StringVar(&tenantBackupField, "tenant-field", "tenant_key", "BSON field name for tenant filter")
	backupTenant.Flags().BoolVar(&tenantBackupNumeric, "tenant-key-numeric", false, "Treat tenant-key as JSON number in query")
	backupTenant.Flags().BoolVar(&tenantCleanupLocal, "tenant-cleanup-local", false, "Remove local dump dir after successful OSS upload")
	_ = backupTenant.MarkFlagRequired("tenant-key")
	_ = backupTenant.MarkFlagRequired("tenant-collections")

	backupCmd.AddCommand(backupFull, backupIncr, backupTenant)

	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Legacy chain restore, or OSS tenant import when --restore-tenant-* / --restore-created-at set",
		RunE:  runRestore,
	}
	restoreCmd.Flags().BoolVar(&restoreForce, "force", false, "Required to confirm restore against live URI")
	restoreCmd.Flags().BoolVar(&restoreDrop, "drop", false, "Chain: --drop on first full only. Tenant: pass --drop to mongorestore")
	restoreCmd.Flags().StringVar(&restoreTenantKey, "restore-tenant-key", "", "OSS tenant import: tenant key")
	restoreCmd.Flags().StringVar(&restoreCreatedAt, "restore-created-at", "", "OSS tenant import: same --created-at as export (YYYY-MM-DD or RFC3339)")
	restoreCmd.Flags().StringVar(&restoreTenantDate, "restore-tenant-date", "", "Deprecated alias: YYYY-MM-DD only, same as restore-created-at for date-only partitions")
	restoreCmd.Flags().StringVar(&restoreTenantRun, "restore-tenant-run", "", "Specific backup run id; default latest")

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show local chain state (legacy)",
		RunE:  runStatus,
	}

	root.AddCommand(ossCmd, backupCmd, restoreCmd, statusCmd)
	ossCmd.AddCommand(ossExport, ossExportDays, ossImport)
	return root
}

func buildConfig() *config.Root {
	r := &config.Root{
		MongoURI:             coalesce(mongoURI, os.Getenv("MONGOMIG_MONGO_URI")),
		StagingDir:           coalesce(stagingDir, os.Getenv("MONGOMIG_STAGING_DIR")),
		Gzip:                 gzipDump,
		ParallelCollections:  parallelCols,
		DumpTimeout:          dumpTimeout,
		RestoreTimeout:       restoreTimeout,
		MongodumpPath:        strings.TrimSpace(mongodumpBin),
		MongorestorePath:     strings.TrimSpace(mongorestoreBin),
		RemotePrefix:         strings.Trim(remotePrefix, "/"),
	}
	r.FromEnv()

	ep := coalesce(s3Endpoint, os.Getenv("MONGOMIG_S3_ENDPOINT"), os.Getenv("S3_ENDPOINT"))
	if ep != "" {
		reg := coalesce(s3Region, os.Getenv("MONGOMIG_S3_REGION"), os.Getenv("S3_REGION"), "us-east-1")
		buck := coalesce(s3Bucket, os.Getenv("MONGOMIG_S3_BUCKET"), os.Getenv("S3_BUCKET"))
		r.S3 = &config.S3{
			Endpoint:        ep,
			Region:          reg,
			Bucket:          buck,
			AccessKeyID:     coalesce(s3AccessKey, os.Getenv("ACCESS_KEY_ID")),
			SecretAccessKey: coalesce(s3SecretKey, os.Getenv("SECRET_ACCESS_KEY")),
			UsePathStyle:    s3PathStyle,
		}
	}
	r.FromEnv()
	return r
}

func coalesce(vals ...string) string {
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}

func resolveTenantPartitionInput(createdAtFlag, tenantDateFallback string) (time.Time, error) {
	if strings.TrimSpace(createdAtFlag) != "" {
		return backup.ParsePartitionCreatedAt(createdAtFlag)
	}
	if strings.TrimSpace(tenantDateFallback) != "" {
		return backup.ParsePartitionCreatedAt(tenantDateFallback)
	}
	return backup.ParsePartitionCreatedAt(time.Now().UTC().Format("2006-01-02"))
}

func runOSSExport(cmd *cobra.Command, args []string) error {
	cfg := buildConfig()
	if err := cfg.Validate(); err != nil {
		return err
	}
	part, err := backup.ParsePartitionCreatedAt(ossExportCreatedAt)
	if err != nil {
		return err
	}
	specs, err := backup.ParseCollectionSpecs(tenantBackupCollections)
	if err != nil {
		return err
	}
	ctx := context.Background()
	remote, err := storage.NewRemote(ctx, cfg)
	if err != nil {
		return err
	}
	m, err := backup.NewRunner(cfg, remote).RunTenantSnapshot(ctx, tenantBackupKey, part, specs, tenantBackupField, tenantBackupNumeric, tenantCleanupLocal)
	if err != nil {
		return err
	}
	seg, err := backup.SanitizeTenantPath(tenantBackupKey)
	if err != nil {
		return err
	}
	ossRun := filepath.ToSlash(filepath.Join(cfg.RemotePrefix, seg, m.PartitionPathSegment, m.BackupID))
	fmt.Printf("oss export ok: tenant=%s partition=%s backup_id=%s oss=%s\n", m.TenantKey, m.PartitionPathSegment, m.BackupID, ossRun)
	return nil
}

func runOSSExportDays(cmd *cobra.Command, args []string) error {
	cfg := buildConfig()
	if err := cfg.Validate(); err != nil {
		return err
	}
	if !cfg.S3Enabled() {
		return fmt.Errorf("oss export-days requires S3 (--s3-endpoint and --s3-bucket)")
	}
	specs, err := backup.ParseCollectionSpecs(tenantBackupCollections)
	if err != nil {
		return err
	}
	ctx := context.Background()
	remote, err := storage.NewRemote(ctx, cfg)
	if err != nil {
		return err
	}
	opt := backup.TenantDaySyncOptions{
		TenantKey:     tenantBackupKey,
		Collections:   specs,
		TenantField:   tenantBackupField,
		TenantNumeric: tenantBackupNumeric,
		TimeField:     ossExportDaysTimeField,
		Timezone:      ossExportDaysTZ,
		FromDay:       ossExportDaysFrom,
		ToDay:         ossExportDaysTo,
		SyncJobID:     ossSyncJobID,
		CleanupLocal:  tenantCleanupLocal,
		DryRun:        ossExportDaysDryRun,
		RedoFailed:    ossExportDaysRedoFailed,
		SkipOKDays:    !ossExportDaysNoSkipOK,
	}
	return backup.NewRunner(cfg, remote).RunTenantDayOSSSync(ctx, opt)
}

func runOSSImport(cmd *cobra.Command, args []string) error {
	if !restoreForce {
		return fmt.Errorf("refusing import without --force")
	}
	cfg := buildConfig()
	if err := cfg.Validate(); err != nil {
		return err
	}
	ctx := context.Background()
	remote, err := storage.NewRemote(ctx, cfg)
	if err != nil {
		return err
	}
	return backup.NewRestoreRunner(cfg, remote).RestoreTenant(ctx, restoreTenantKey, restoreCreatedAt, restoreTenantRun, restoreDrop)
}

func runBackupFull(cmd *cobra.Command, args []string) error {
	cfg := buildConfig()
	if err := cfg.Validate(); err != nil {
		return err
	}
	ctx := context.Background()
	remote, err := storage.NewRemote(ctx, cfg)
	if err != nil {
		return err
	}
	m, err := backup.NewRunner(cfg, remote).RunFull(ctx, chain)
	if err != nil {
		return err
	}
	fmt.Printf("full backup ok: id=%s last_oplog_ts=%v\n", m.BackupID, m.LastOplogTS)
	return nil
}

func runBackupIncremental(cmd *cobra.Command, args []string) error {
	cfg := buildConfig()
	if err := cfg.Validate(); err != nil {
		return err
	}
	ctx := context.Background()
	remote, err := storage.NewRemote(ctx, cfg)
	if err != nil {
		return err
	}
	m, err := backup.NewRunner(cfg, remote).RunIncremental(ctx, chain)
	if err != nil {
		return err
	}
	fmt.Printf("incremental backup ok: id=%s last_oplog_ts=%v\n", m.BackupID, m.LastOplogTS)
	return nil
}

func runRestore(cmd *cobra.Command, args []string) error {
	if !restoreForce {
		return fmt.Errorf("refusing restore without --force")
	}
	cfg := buildConfig()
	if err := cfg.Validate(); err != nil {
		return err
	}
	ctx := context.Background()
	remote, err := storage.NewRemote(ctx, cfg)
	if err != nil {
		return err
	}
	partIn := strings.TrimSpace(restoreCreatedAt)
	if partIn == "" {
		partIn = strings.TrimSpace(restoreTenantDate)
	}
	if strings.TrimSpace(restoreTenantKey) != "" {
		if partIn == "" {
			return fmt.Errorf("tenant OSS import requires --restore-created-at or --restore-tenant-date (same partition as export)")
		}
		return backup.NewRestoreRunner(cfg, remote).RestoreTenant(ctx, restoreTenantKey, partIn, restoreTenantRun, restoreDrop)
	}
	return backup.NewRestoreRunner(cfg, remote).RestoreChain(ctx, chain, restoreDrop)
}

func runBackupTenant(cmd *cobra.Command, args []string) error {
	cfg := buildConfig()
	if err := cfg.Validate(); err != nil {
		return err
	}
	specs, err := backup.ParseCollectionSpecs(tenantBackupCollections)
	if err != nil {
		return err
	}
	part, err := resolveTenantPartitionInput(tenantCreatedAt, tenantBackupDate)
	if err != nil {
		return err
	}
	ctx := context.Background()
	remote, err := storage.NewRemote(ctx, cfg)
	if err != nil {
		return err
	}
	m, err := backup.NewRunner(cfg, remote).RunTenantSnapshot(ctx, tenantBackupKey, part, specs, tenantBackupField, tenantBackupNumeric, tenantCleanupLocal)
	if err != nil {
		return err
	}
	seg, err := backup.SanitizeTenantPath(tenantBackupKey)
	if err != nil {
		return err
	}
	ossRun := filepath.ToSlash(filepath.Join(cfg.RemotePrefix, seg, m.PartitionPathSegment, m.BackupID))
	fmt.Printf("tenant export ok: tenant=%s partition=%s backup_id=%s oss=%s\n", m.TenantKey, m.PartitionPathSegment, m.BackupID, ossRun)
	return nil
}

func runStatus(cmd *cobra.Command, args []string) error {
	cfg := buildConfig()
	if err := cfg.Validate(); err != nil {
		return err
	}
	s, err := backup.ChainStatus(cfg, chain)
	if err != nil {
		return err
	}
	fmt.Println(s)
	return nil
}
