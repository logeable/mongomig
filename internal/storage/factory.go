package storage

import (
	"context"
	"fmt"

	"github.com/logeable/mongomig/internal/config"
	s3store "github.com/logeable/mongomig/internal/storage/s3"
)

// NewRemote returns S3 backend when enabled, otherwise NopRemote.
func NewRemote(ctx context.Context, cfg *config.Root) (Backend, error) {
	if cfg == nil || !cfg.S3Enabled() {
		return NopRemote{}, nil
	}
	b, err := s3store.New(ctx, cfg.S3)
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	return b, nil
}
