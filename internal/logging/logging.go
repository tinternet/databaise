package logging

import (
	"fmt"
	"log"
	"os"
)

var std = log.New(os.Stdout, "", log.LstdFlags|log.Lmsgprefix|log.Lshortfile)

// SetOutput sets the output destination for the default logger.
func SetOutput(w *os.File) {
	std.SetOutput(w)
}

// New creates a prefixed logger.
func New(prefix string) *log.Logger {
	return log.New(std.Writer(), fmt.Sprintf("[%s] ", prefix), std.Flags())
}

// Package-level functions for unprefixed logging
func Info(msg string, args ...any) {
	std.Output(2, fmt.Sprintf(msg, args...))
}

func Warn(msg string, args ...any) {
	std.Output(2, fmt.Sprintf("WARN: "+msg, args...))
}

func Error(msg string, args ...any) {
	std.Output(2, fmt.Sprintf("ERROR: "+msg, args...))
}

func Fatal(msg string, args ...any) {
	std.Output(2, fmt.Sprintf("FATAL: "+msg, args...))
	os.Exit(1)
}
