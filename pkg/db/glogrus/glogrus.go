// Package glogrus provides a gorm logger that wraps a logrus.Logger.
package glogrus

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
	gutils "gorm.io/gorm/utils"
)

// Config is used to configure a gorm Logger that wraps a logrus.Logger.
type Config struct {
	// Logger is the logrus logger to use. If nil, logrus.StandardLogger() is used.
	Logger *logrus.Logger

	// SlowThreshold is the threshold for logging slow queries. If zero, 500ms is used.
	SlowThreshold time.Duration

	// IgnoreRecordNotFoundError determines if `gorm.ErrRecordNotFound` errors are logged.
	// `gorm.ErrRecordNotFound` logging is disabled IFF IgnoreRecordNotFoundError is true.
	IgnoreRecordNotFoundError bool

	// LogSQL determines if SQL queries are included in the log output produced by calls to Logger.Trace.
	//
	// `gorm.ErrRecordNotFound` logging is disabled IFF IgnoreRecordNotFoundError is true.
	LogSQL bool
}

// New returns a new *Logger configured with the given config.
func New(cfg Config) *Logger {
	l := &Logger{
		logger:                    cfg.Logger,
		slowThreshold:             cfg.SlowThreshold,
		ignoreRecordNotFoundError: cfg.IgnoreRecordNotFoundError,
		logSQL:                    cfg.LogSQL,
	}
	l.complete()

	return l
}

// Logger is a gorm logger that wraps a logrus.Logger.
// The zero value of Logger is valid and writes to logrus.StandardLogger() with default settings.
type Logger struct {
	logger                    *logrus.Logger
	once                      sync.Once
	slowThreshold             time.Duration
	ignoreRecordNotFoundError bool
	logSQL                    bool
}

func (l *Logger) LogMode(glogger.LogLevel) glogger.Interface {
	l.complete()
	return l
}

func (l *Logger) Info(ctx context.Context, s string, args ...any) {
	l.complete()
	l.logger.WithContext(ctx).Infof(s, args...)
}

func (l *Logger) Warn(ctx context.Context, s string, args ...any) {
	l.complete()
	l.logger.WithContext(ctx).Warnf(s, args...)
}

func (l *Logger) Error(ctx context.Context, s string, args ...any) {
	l.complete()
	l.logger.WithContext(ctx).Errorf(s, args...)
}

func (l *Logger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	l.complete()
	elapsed := time.Since(begin)
	sql, affected := fc()

	log := l.logger.WithContext(ctx).WithFields(logrus.Fields{
		"elapsed":  elapsed,
		"affected": affected,
		"caller":   gutils.FileWithLineNum(),
	})

	if l.logSQL {
		// Add the SQL query to all log levels if the logger is set to Trace.
		log = log.WithField("sql", sql)
	}

	if err != nil && !(l.ignoreRecordNotFoundError && errors.Is(err, gorm.ErrRecordNotFound)) {
		log.WithError(err).Error("sql query error")
		return
	}

	if l.slowThreshold != 0 && elapsed > l.slowThreshold {
		log.Info("sql query slow")
		return
	}

	log.Trace("sql query executed")
}

// complete ensures that the Logger is fully initialized.
// It's idempotent and should be called at the beginning of every method exported by Logger.
func (l *Logger) complete() {
	l.once.Do(func() {
		if l.logger == nil {
			l.logger = logrus.StandardLogger()
		}
		if l.slowThreshold == 0 {
			l.slowThreshold = 500 * time.Millisecond
		}
	})
}
