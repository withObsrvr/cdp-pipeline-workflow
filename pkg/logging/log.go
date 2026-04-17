// Package logging provides verbosity-controlled logging for the CDP pipeline.
//
// Verbosity levels:
//   - Quiet (0): Errors only - for production use
//   - Normal (1): Startup/shutdown, key events, warnings
//   - Verbose (2): Debug details - for development/troubleshooting
//
// Set verbosity via LOG_LEVEL environment variable:
//   - "quiet", "error", "" (default) -> VerbosityQuiet
//   - "normal", "info" -> VerbosityNormal
//   - "verbose", "debug" -> VerbosityVerbose
package logging

import (
	"log"
	"os"
	"strings"
)

// Verbosity represents the logging verbosity level.
type Verbosity int

const (
	// VerbosityQuiet logs errors only - suitable for production.
	VerbosityQuiet Verbosity = 0
	// VerbosityNormal logs startup/shutdown, key events, and warnings.
	VerbosityNormal Verbosity = 1
	// VerbosityVerbose logs debug details - suitable for development.
	VerbosityVerbose Verbosity = 2
)

var verbosity = VerbosityQuiet

func init() {
	level := strings.ToLower(os.Getenv("LOG_LEVEL"))
	switch level {
	case "verbose", "debug":
		verbosity = VerbosityVerbose
	case "normal", "info":
		verbosity = VerbosityNormal
	default:
		verbosity = VerbosityQuiet
	}
}

// Debug logs a message only when verbosity is VerbosityVerbose.
// Use for per-ledger/transaction details, loop iterations, field extraction.
func Debug(format string, v ...any) {
	if verbosity >= VerbosityVerbose {
		log.Printf(format, v...)
	}
}

// Info logs a message when verbosity is VerbosityNormal or higher.
// Use for startup, shutdown, milestones, and configuration messages.
func Info(format string, v ...any) {
	if verbosity >= VerbosityNormal {
		log.Printf(format, v...)
	}
}

// Warn logs a warning message when verbosity is VerbosityNormal or higher.
// Use for non-critical issues that should be addressed.
func Warn(format string, v ...any) {
	if verbosity >= VerbosityNormal {
		log.Printf("[WARN] "+format, v...)
	}
}

// Error logs an error message at all verbosity levels.
// Use for failures and unexpected states that require attention.
func Error(format string, v ...any) {
	log.Printf("[ERROR] "+format, v...)
}

// SetVerbosity sets the logging verbosity level programmatically.
// This can be used to override the LOG_LEVEL environment variable.
func SetVerbosity(v Verbosity) {
	verbosity = v
}

// GetVerbosity returns the current verbosity level.
func GetVerbosity() Verbosity {
	return verbosity
}

// IsDebugEnabled returns true if debug logging is enabled.
// Use this to guard expensive formatting operations:
//
//	if logging.IsDebugEnabled() {
//	    logging.Debug("Complex data: %s", expensiveFormat(data))
//	}
func IsDebugEnabled() bool {
	return verbosity >= VerbosityVerbose
}

// IsInfoEnabled returns true if info logging is enabled.
func IsInfoEnabled() bool {
	return verbosity >= VerbosityNormal
}
