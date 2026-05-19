package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newRoot() *cobra.Command {
	v := viper.New()
	root := &cobra.Command{
		Use:   "mongomig",
		Short: "MongoDB collection backup to S3-compatible OSS (UTC hourly, tenant discover)",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return v.BindPFlags(cmd.PersistentFlags())
		},
	}

	pf := root.PersistentFlags()
	pf.String("config", "", "Config file path (default: ./mongomig.yaml in CWD)")
	pf.String("mongo-uri", "", "MongoDB URI (MONGOMIG_MONGO_URI)")
	pf.String("staging-dir", "", "Local staging directory")
	pf.Bool("gzip", true, "mongodump --gzip")
	pf.Duration("dump-timeout", 0, "Per-dump timeout (0=24h)")
	pf.String("mongodump-path", "", "mongodump binary path")
	pf.String("remote-prefix", "mongomig", "OSS key prefix")
	pf.String("s3-endpoint", "", "S3-compatible endpoint")
	pf.String("s3-region", "us-east-1", "S3 region")
	pf.String("s3-bucket", "", "S3 bucket")
	pf.Bool("s3-path-style", false, "S3 path-style addressing")
	pf.String("s3-access-key", "", "S3 access key")
	pf.String("s3-secret-key", "", "S3 secret key")
	pf.String("log-level", "info", "Log level: debug, info, warn, error")

	_ = v.BindPFlag("config", pf.Lookup("config"))
	_ = v.BindPFlag("mongo_uri", pf.Lookup("mongo-uri"))
	_ = v.BindPFlag("staging_dir", pf.Lookup("staging-dir"))
	_ = v.BindPFlag("gzip", pf.Lookup("gzip"))
	_ = v.BindPFlag("dump_timeout", pf.Lookup("dump-timeout"))
	_ = v.BindPFlag("mongodump_path", pf.Lookup("mongodump-path"))
	_ = v.BindPFlag("remote_prefix", pf.Lookup("remote-prefix"))
	_ = v.BindPFlag("s3.endpoint", pf.Lookup("s3-endpoint"))
	_ = v.BindPFlag("s3.region", pf.Lookup("s3-region"))
	_ = v.BindPFlag("s3.bucket", pf.Lookup("s3-bucket"))
	_ = v.BindPFlag("s3.use_path_style", pf.Lookup("s3-path-style"))
	_ = v.BindPFlag("s3.access_key_id", pf.Lookup("s3-access-key"))
	_ = v.BindPFlag("s3.secret_access_key", pf.Lookup("s3-secret-key"))
	_ = v.BindPFlag("log_level", pf.Lookup("log-level"))

	root.AddCommand(newBackupCmd(v), newRestoreCmd(v), newStatusCmd(v), newRepairCmd(v))
	return root
}
