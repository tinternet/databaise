package logging

import (
	"time"

	"gorm.io/gorm/logger"
)

var gormLogLevel = logger.Silent

// SetGormLogLevel sets the global GORM log level.
func SetGormLogLevel(level logger.LogLevel) {
	gormLogLevel = level
}

// NewGormLogger creates a GORM logger that writes to the app's logging output.
// In STDIO mode, this will be stderr (set via SetOutput before backends init).
func NewGormLogger() logger.Interface {
	return logger.New(
		std,
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  gormLogLevel,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	)
}

// ParseGormLogLevel converts a string log level to GORM's LogLevel type.
func ParseGormLogLevel(level string) logger.LogLevel {
	switch level {
	case "silent":
		return logger.Silent
	case "error":
		return logger.Error
	case "warn":
		return logger.Warn
	case "info":
		return logger.Info
	default:
		return logger.Silent
	}
}
