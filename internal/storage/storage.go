package storage

import (
	"context"
)

// Backend is local disk and/or remote object storage.
type Backend interface {
	// UploadFile uploads a single file from local path to remote key (if remote configured).
	UploadFile(ctx context.Context, localPath, key string) error
	// DownloadFile downloads remote key to local path.
	DownloadFile(ctx context.Context, key, localPath string) error
	// ListKeys returns object keys under prefix (recursive, relative to prefix).
	ListKeys(ctx context.Context, prefix string) ([]string, error)
	// DeletePrefix removes all keys with prefix (best-effort).
	DeletePrefix(ctx context.Context, prefix string) error
}

// NopRemote implements Backend with local-only no-op for remote operations.
type NopRemote struct{}

func (NopRemote) UploadFile(ctx context.Context, localPath, key string) error {
	return nil
}

func (NopRemote) DownloadFile(ctx context.Context, key, localPath string) error {
	return nil
}

func (NopRemote) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}

func (NopRemote) DeletePrefix(ctx context.Context, prefix string) error {
	return nil
}
