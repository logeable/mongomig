package log

import (
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New builds a production-style zap logger for the given level name.
func New(level string) (*zap.Logger, error) {
	lvl := zapcore.InfoLevel
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		lvl = zapcore.DebugLevel
	case "warn", "warning":
		lvl = zapcore.WarnLevel
	case "error":
		lvl = zapcore.ErrorLevel
	case "info", "":
		lvl = zapcore.InfoLevel
	default:
		lvl = zapcore.InfoLevel
	}
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	cfg.Encoding = "console"
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	return cfg.Build()
}
