package checkpoint

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteAtomic writes data to a file atomically using write-then-rename pattern
// This prevents corruption from partial writes or crashes during write operations
func WriteAtomic(filePath string, data []byte) error {
	// Ensure the directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write to temporary file first
	tmpPath := filePath + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Write data to temp file
	_, err = tmpFile.Write(data)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write to temp file: %w", err)
	}

	// Sync to disk to ensure data is persisted
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	// Close temp file
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomically rename temp file to final file
	// This is atomic on POSIX systems (Linux, macOS)
	if err := os.Rename(tmpPath, filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file to %s: %w", filePath, err)
	}

	return nil
}
