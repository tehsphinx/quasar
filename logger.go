// Copyright (c) RealTyme SA. All rights reserved.

package quasar

import (
	"io"
	"log"
	"log/slog"

	"github.com/hashicorp/go-hclog"
)

// slogAdapter wraps slog.Logger to implement hclog.Logger.
type slogAdapter struct {
	logger *slog.Logger
}

func (l *slogAdapter) Log(level hclog.Level, msg string, args ...interface{}) {
	var slogLevel slog.Level
	switch level {
	case hclog.Trace, hclog.Debug:
		slogLevel = slog.LevelDebug
	case hclog.Info:
		slogLevel = slog.LevelInfo
	case hclog.Warn:
		slogLevel = slog.LevelWarn
	case hclog.Error:
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}
	l.logger.Log(nil, slogLevel, msg, args...)
}

func (l *slogAdapter) Trace(msg string, args ...interface{}) {
	l.logger.Debug(msg, args...)
}

func (l *slogAdapter) Debug(msg string, args ...interface{}) {
	l.logger.Debug(msg, args...)
}

func (l *slogAdapter) Info(msg string, args ...interface{}) {
	l.logger.Info(msg, args...)
}

func (l *slogAdapter) Warn(msg string, args ...interface{}) {
	l.logger.Warn(msg, args...)
}

func (l *slogAdapter) Error(msg string, args ...interface{}) {
	l.logger.Error(msg, args...)
}

func (l *slogAdapter) IsTrace() bool {
	return l.logger.Enabled(nil, slog.LevelDebug)
}

func (l *slogAdapter) IsDebug() bool {
	return l.logger.Enabled(nil, slog.LevelDebug)
}

func (l *slogAdapter) IsInfo() bool {
	return l.logger.Enabled(nil, slog.LevelInfo)
}

func (l *slogAdapter) IsWarn() bool {
	return l.logger.Enabled(nil, slog.LevelWarn)
}

func (l *slogAdapter) IsError() bool {
	return l.logger.Enabled(nil, slog.LevelError)
}

func (l *slogAdapter) ImpliedArgs() []interface{} {
	return nil
}

func (l *slogAdapter) With(args ...interface{}) hclog.Logger {
	return &slogAdapter{logger: l.logger.With(args...)}
}

func (l *slogAdapter) Name() string {
	return ""
}

func (l *slogAdapter) Named(name string) hclog.Logger {
	return &slogAdapter{logger: l.logger.With("name", name)}
}

func (l *slogAdapter) ResetNamed(name string) hclog.Logger {
	return &slogAdapter{logger: l.logger.With("name", name)}
}

func (l *slogAdapter) SetLevel(level hclog.Level) {
	// slog doesn't support dynamic level changes on the logger itself
}

func (l *slogAdapter) GetLevel() hclog.Level {
	// Approximate based on what's enabled
	if l.logger.Enabled(nil, slog.LevelDebug) {
		return hclog.Debug
	}
	if l.logger.Enabled(nil, slog.LevelInfo) {
		return hclog.Info
	}
	if l.logger.Enabled(nil, slog.LevelWarn) {
		return hclog.Warn
	}
	return hclog.Error
}

func (l *slogAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return slog.NewLogLogger(l.logger.Handler(), slog.LevelInfo)
}

func (l *slogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return &slogWriter{logger: l.logger}
}

type slogWriter struct {
	logger *slog.Logger
}

func (w *slogWriter) Write(p []byte) (n int, err error) {
	w.logger.Info(string(p))
	return len(p), nil
}
