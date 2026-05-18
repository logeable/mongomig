package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/logeable/mongomig/internal/backup"
	"github.com/logeable/mongomig/internal/config"
	mongolog "github.com/logeable/mongomig/internal/log"
	"github.com/logeable/mongomig/internal/storage"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type ossRun struct {
	cfg    *config.Root
	remote storage.Backend
	meta   *backup.MetaStore
	specs  []backup.NSSpec
	log    *zap.Logger
}

func prepareOSSRun(ctx context.Context, v *viper.Viper, dbName, collections string) (*ossRun, error) {
	cfg, err := config.LoadViper(v)
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !cfg.S3Enabled() {
		return nil, fmt.Errorf("需要配置 S3/OSS（mongomig.yaml 中 s3.endpoint 与 bucket）才能扫描远程 meta")
	}
	if err := cfg.EnsureStaging(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(dbName) == "" {
		dbName = strings.TrimSpace(cfg.DB)
	}
	if strings.TrimSpace(dbName) == "" {
		return nil, fmt.Errorf("--db 必填（或在 mongomig.yaml 中设置 db）")
	}
	logger, err := mongolog.New(cfg.LogLevel)
	if err != nil {
		return nil, err
	}
	specs, err := backup.ResolveCollectionSpecs(ctx, logger, cfg.MongoURI, dbName, collections)
	if err != nil {
		_ = logger.Sync()
		return nil, err
	}
	remote, err := storage.NewRemote(ctx, cfg)
	if err != nil {
		_ = logger.Sync()
		return nil, err
	}
	meta, err := backup.NewMetaStore(remote, cfg.StagingDir)
	if err != nil {
		_ = logger.Sync()
		return nil, err
	}
	return &ossRun{cfg: cfg, remote: remote, meta: meta, specs: specs, log: logger}, nil
}

func (r *ossRun) close() {
	if r != nil && r.log != nil {
		_ = r.log.Sync()
	}
}
