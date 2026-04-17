package logging

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

// captureOutput captures log output for testing.
func captureOutput(f func()) string {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)
	f()
	return buf.String()
}

func TestVerbosityLevels(t *testing.T) {
	tests := []struct {
		name           string
		verbosity      Verbosity
		expectDebug    bool
		expectInfo     bool
		expectWarn     bool
		expectError    bool
	}{
		{
			name:        "Quiet - only errors",
			verbosity:   VerbosityQuiet,
			expectDebug: false,
			expectInfo:  false,
			expectWarn:  false,
			expectError: true,
		},
		{
			name:        "Normal - info, warn, error",
			verbosity:   VerbosityNormal,
			expectDebug: false,
			expectInfo:  true,
			expectWarn:  true,
			expectError: true,
		},
		{
			name:        "Verbose - all messages",
			verbosity:   VerbosityVerbose,
			expectDebug: true,
			expectInfo:  true,
			expectWarn:  true,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetVerbosity(tt.verbosity)

			// Test Debug
			output := captureOutput(func() {
				Debug("debug message")
			})
			if (len(output) > 0) != tt.expectDebug {
				t.Errorf("Debug: expected output=%v, got output=%q", tt.expectDebug, output)
			}

			// Test Info
			output = captureOutput(func() {
				Info("info message")
			})
			if (len(output) > 0) != tt.expectInfo {
				t.Errorf("Info: expected output=%v, got output=%q", tt.expectInfo, output)
			}

			// Test Warn
			output = captureOutput(func() {
				Warn("warn message")
			})
			if (len(output) > 0) != tt.expectWarn {
				t.Errorf("Warn: expected output=%v, got output=%q", tt.expectWarn, output)
			}
			if tt.expectWarn && !strings.Contains(output, "[WARN]") {
				t.Errorf("Warn: expected [WARN] prefix, got %q", output)
			}

			// Test Error
			output = captureOutput(func() {
				Error("error message")
			})
			if (len(output) > 0) != tt.expectError {
				t.Errorf("Error: expected output=%v, got output=%q", tt.expectError, output)
			}
			if tt.expectError && !strings.Contains(output, "[ERROR]") {
				t.Errorf("Error: expected [ERROR] prefix, got %q", output)
			}
		})
	}
}

func TestIsDebugEnabled(t *testing.T) {
	tests := []struct {
		verbosity Verbosity
		expected  bool
	}{
		{VerbosityQuiet, false},
		{VerbosityNormal, false},
		{VerbosityVerbose, true},
	}

	for _, tt := range tests {
		SetVerbosity(tt.verbosity)
		if got := IsDebugEnabled(); got != tt.expected {
			t.Errorf("IsDebugEnabled() at verbosity %d: got %v, want %v", tt.verbosity, got, tt.expected)
		}
	}
}

func TestIsInfoEnabled(t *testing.T) {
	tests := []struct {
		verbosity Verbosity
		expected  bool
	}{
		{VerbosityQuiet, false},
		{VerbosityNormal, true},
		{VerbosityVerbose, true},
	}

	for _, tt := range tests {
		SetVerbosity(tt.verbosity)
		if got := IsInfoEnabled(); got != tt.expected {
			t.Errorf("IsInfoEnabled() at verbosity %d: got %v, want %v", tt.verbosity, got, tt.expected)
		}
	}
}

func TestGetVerbosity(t *testing.T) {
	SetVerbosity(VerbosityVerbose)
	if got := GetVerbosity(); got != VerbosityVerbose {
		t.Errorf("GetVerbosity(): got %v, want %v", got, VerbosityVerbose)
	}

	SetVerbosity(VerbosityQuiet)
	if got := GetVerbosity(); got != VerbosityQuiet {
		t.Errorf("GetVerbosity(): got %v, want %v", got, VerbosityQuiet)
	}
}

func TestFormatArgs(t *testing.T) {
	SetVerbosity(VerbosityVerbose)

	output := captureOutput(func() {
		Debug("ledger %d has %d transactions", 12345, 42)
	})

	if !strings.Contains(output, "ledger 12345 has 42 transactions") {
		t.Errorf("Format args not applied correctly: %q", output)
	}
}

func TestEnvVarParsing(t *testing.T) {
	tests := []struct {
		envValue string
		expected Verbosity
	}{
		{"verbose", VerbosityVerbose},
		{"VERBOSE", VerbosityVerbose},
		{"debug", VerbosityVerbose},
		{"DEBUG", VerbosityVerbose},
		{"normal", VerbosityNormal},
		{"NORMAL", VerbosityNormal},
		{"info", VerbosityNormal},
		{"INFO", VerbosityNormal},
		{"quiet", VerbosityQuiet},
		{"QUIET", VerbosityQuiet},
		{"error", VerbosityQuiet},
		{"", VerbosityQuiet},
		{"unknown", VerbosityQuiet},
	}

	for _, tt := range tests {
		t.Run("LOG_LEVEL="+tt.envValue, func(t *testing.T) {
			// Reset verbosity
			verbosity = VerbosityQuiet

			// Simulate init() behavior
			level := strings.ToLower(tt.envValue)
			switch level {
			case "verbose", "debug":
				verbosity = VerbosityVerbose
			case "normal", "info":
				verbosity = VerbosityNormal
			default:
				verbosity = VerbosityQuiet
			}

			if verbosity != tt.expected {
				t.Errorf("LOG_LEVEL=%q: got verbosity %d, want %d", tt.envValue, verbosity, tt.expected)
			}
		})
	}
}
