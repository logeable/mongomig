package config

import (
	"os"
	"strings"

	"github.com/spf13/viper"
)

// LoadViper reads defaults, optional config file, .env, environment, and CLI-bound viper keys.
func LoadViper(v *viper.Viper) (*Root, error) {
	if v == nil {
		v = viper.New()
	}
	v.SetEnvPrefix("MONGOMIG")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	v.AutomaticEnv()

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

	_ = v.BindEnv("mongo_uri")
	_ = v.BindEnv("staging_dir")
	_ = v.BindEnv("remote_prefix")
	_ = v.BindEnv("mongodump_path")
	_ = v.BindEnv("mongorestore_path")
	_ = v.BindEnv("s3.endpoint", "MONGOMIG_S3_ENDPOINT", "S3_ENDPOINT")
	_ = v.BindEnv("s3.region", "MONGOMIG_S3_REGION", "S3_REGION")
	_ = v.BindEnv("s3.bucket", "MONGOMIG_S3_BUCKET", "S3_BUCKET")
	_ = v.BindEnv("s3.access_key_id", "MONGOMIG_S3_ACCESS_KEY_ID", "S3_ACCESS_KEY_ID", "ACCESS_KEY_ID")
	_ = v.BindEnv("s3.secret_access_key", "MONGOMIG_S3_SECRET_ACCESS_KEY", "S3_SECRET_ACCESS_KEY", "SECRET_ACCESS_KEY")
	_ = v.BindEnv("s3.use_path_style", "MONGOMIG_S3_USE_PATH_STYLE", "S3_USE_PATH_STYLE")

	v.SetConfigName("mongomig")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	_ = v.ReadInConfig()

	if _, err := os.Stat(".env"); err == nil {
		env := viper.New()
		env.SetConfigFile(".env")
		env.SetConfigType("env")
		if err := env.ReadInConfig(); err == nil {
			for _, k := range env.AllKeys() {
				if !v.IsSet(k) && env.GetString(k) != "" {
					v.Set(k, env.GetString(k))
				}
			}
			overlayEnvAliases(v, env)
		}
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
	}
	if ep := v.GetString("s3.endpoint"); ep != "" {
		root.S3 = &S3{
			Endpoint:        ep,
			Region:          v.GetString("s3.region"),
			Bucket:          v.GetString("s3.bucket"),
			AccessKeyID:     firstNonEmpty(v.GetString("s3.access_key_id"), os.Getenv("ACCESS_KEY_ID")),
			SecretAccessKey: firstNonEmpty(v.GetString("s3.secret_access_key"), os.Getenv("SECRET_ACCESS_KEY")),
			UsePathStyle:    v.GetBool("s3.use_path_style"),
		}
	}
	root.FromEnv()
	return root, nil
}

func overlayEnvAliases(v, env *viper.Viper) {
	setIf := func(key string, vals ...string) {
		if v.IsSet(key) {
			return
		}
		for _, k := range vals {
			if s := env.GetString(k); s != "" {
				v.Set(key, s)
				return
			}
		}
	}
	setIf("mongo_uri", "MONGOMIG_MONGO_URI", "MONGO_URI")
	setIf("s3.endpoint", "MONGOMIG_S3_ENDPOINT", "S3_ENDPOINT")
	setIf("s3.bucket", "MONGOMIG_S3_BUCKET", "S3_BUCKET")
	setIf("s3.region", "MONGOMIG_S3_REGION", "S3_REGION")
	setIf("s3.access_key_id", "MONGOMIG_S3_ACCESS_KEY_ID", "S3_ACCESS_KEY_ID", "ACCESS_KEY_ID")
	setIf("s3.secret_access_key", "MONGOMIG_S3_SECRET_ACCESS_KEY", "S3_SECRET_ACCESS_KEY", "SECRET_ACCESS_KEY")
}

func firstNonEmpty(vals ...string) string {
	for _, s := range vals {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}
