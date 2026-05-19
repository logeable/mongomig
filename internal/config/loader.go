package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// LoadViper reads defaults, config file, then CLI flags already bound on v.
// Config file: --config path, else ./mongomig.yaml in the current working directory (optional).
func LoadViper(v *viper.Viper) (*Root, error) {
	if v == nil {
		v = viper.New()
	}

	v.SetDefault("mongo_uri", "")
	v.SetDefault("staging_dir", "./staging")
	v.SetDefault("remote_prefix", "mongomig")
	v.SetDefault("gzip", true)
	v.SetDefault("parallel_collections", 0)
	v.SetDefault("dump_timeout", 0)
	v.SetDefault("restore_timeout", 0)
	v.SetDefault("log_level", "info")
	v.SetDefault("s3.region", "us-east-1")
	v.SetDefault("s3.use_path_style", false)

	if err := readViperConfig(v); err != nil {
		return nil, err
	}

	root := &Root{
		MongoURI:            v.GetString("mongo_uri"),
		MongodumpPath:       v.GetString("mongodump_path"),
		MongorestorePath:    v.GetString("mongorestore_path"),
		StagingDir:          v.GetString("staging_dir"),
		ParallelCollections: v.GetInt("parallel_collections"),
		Gzip:                v.GetBool("gzip"),
		DumpTimeout:         v.GetDuration("dump_timeout"),
		RestoreTimeout:      v.GetDuration("restore_timeout"),
		RemotePrefix:        strings.Trim(v.GetString("remote_prefix"), "/"),
		LogLevel:            v.GetString("log_level"),
		DB:                  strings.TrimSpace(v.GetString("db")),
	}
	if ep := v.GetString("s3.endpoint"); ep != "" {
		root.S3 = &S3{
			Endpoint:        ep,
			Region:          v.GetString("s3.region"),
			Bucket:          v.GetString("s3.bucket"),
			AccessKeyID:     v.GetString("s3.access_key_id"),
			SecretAccessKey: v.GetString("s3.secret_access_key"),
			UsePathStyle:    v.GetBool("s3.use_path_style"),
		}
	}
	return root, nil
}

func readViperConfig(v *viper.Viper) error {
	configPath := strings.TrimSpace(v.GetString("config"))
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return fmt.Errorf("read config %q: %w", configPath, err)
		}
		return nil
	}
	v.SetConfigName("mongomig")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("read config: %w", err)
	}
	return nil
}
