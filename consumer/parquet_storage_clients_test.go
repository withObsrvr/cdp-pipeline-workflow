package consumer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCreateStorageClient(t *testing.T) {
	tests := []struct {
		name    string
		config  SaveToParquetConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid local filesystem",
			config: SaveToParquetConfig{
				StorageType: "FS",
				LocalPath:   t.TempDir(),
			},
			wantErr: false,
		},
		{
			name: "unsupported storage type",
			config: SaveToParquetConfig{
				StorageType: "UNSUPPORTED",
			},
			wantErr: true,
			errMsg:  "unsupported storage type: UNSUPPORTED",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := createStorageClient(tt.config)
			
			if tt.wantErr {
				if err == nil {
					t.Errorf("createStorageClient() error = nil, wantErr %v", tt.wantErr)
				} else if tt.errMsg != "" && err.Error() != tt.errMsg {
					t.Errorf("createStorageClient() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("createStorageClient() error = %v, wantErr %v", err, tt.wantErr)
				}
				if client != nil {
					client.Close()
				}
			}
		})
	}
}

func TestLocalFSClient(t *testing.T) {
	t.Run("create client", func(t *testing.T) {
		tmpDir := t.TempDir()
		client, err := NewLocalFSClient(tmpDir)
		if err != nil {
			t.Fatalf("NewLocalFSClient() error = %v", err)
		}
		defer client.Close()
		
		if client.basePath == "" {
			t.Error("LocalFSClient basePath is empty")
		}
	})
	
	t.Run("write file", func(t *testing.T) {
		tmpDir := t.TempDir()
		client, err := NewLocalFSClient(tmpDir)
		if err != nil {
			t.Fatalf("NewLocalFSClient() error = %v", err)
		}
		defer client.Close()
		
		ctx := context.Background()
		testData := []byte("test parquet data")
		testKey := "year=2024/month=01/test.parquet"
		
		// Write file
		err = client.Write(ctx, testKey, testData)
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		
		// Verify file exists
		expectedPath := filepath.Join(tmpDir, testKey)
		data, err := os.ReadFile(expectedPath)
		if err != nil {
			t.Fatalf("Failed to read written file: %v", err)
		}
		
		if string(data) != string(testData) {
			t.Errorf("File content = %s, want %s", string(data), string(testData))
		}
	})
	
	t.Run("atomic write", func(t *testing.T) {
		tmpDir := t.TempDir()
		client, err := NewLocalFSClient(tmpDir)
		if err != nil {
			t.Fatalf("NewLocalFSClient() error = %v", err)
		}
		defer client.Close()
		
		ctx := context.Background()
		testKey := "test-atomic.parquet"
		
		// Write initial data
		initialData := []byte("initial data")
		err = client.Write(ctx, testKey, initialData)
		if err != nil {
			t.Fatalf("Write() initial error = %v", err)
		}
		
		// Overwrite with new data
		newData := []byte("new data")
		err = client.Write(ctx, testKey, newData)
		if err != nil {
			t.Fatalf("Write() overwrite error = %v", err)
		}
		
		// Verify new data
		expectedPath := filepath.Join(tmpDir, testKey)
		data, err := os.ReadFile(expectedPath)
		if err != nil {
			t.Fatalf("Failed to read written file: %v", err)
		}
		
		if string(data) != string(newData) {
			t.Errorf("File content = %s, want %s", string(data), string(newData))
		}
	})
	
	t.Run("directory creation", func(t *testing.T) {
		tmpDir := t.TempDir()
		client, err := NewLocalFSClient(tmpDir)
		if err != nil {
			t.Fatalf("NewLocalFSClient() error = %v", err)
		}
		defer client.Close()
		
		ctx := context.Background()
		testData := []byte("test data")
		testKey := "deep/nested/directory/structure/file.parquet"
		
		// Write file in nested directory
		err = client.Write(ctx, testKey, testData)
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		
		// Verify directory structure exists
		expectedPath := filepath.Join(tmpDir, testKey)
		if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
			t.Errorf("Expected file does not exist: %s", expectedPath)
		}
	})
	
	t.Run("prevent directory traversal", func(t *testing.T) {
		tmpDir := t.TempDir()
		client, err := NewLocalFSClient(tmpDir)
		if err != nil {
			t.Fatalf("NewLocalFSClient() error = %v", err)
		}
		defer client.Close()
		
		ctx := context.Background()
		testData := []byte("test data")
		
		// Test various malicious paths
		maliciousPaths := []string{
			"../../../etc/passwd",
			"/etc/passwd",
			"..\\..\\windows\\system32\\config",
			"./../../sensitive.txt",
		}
		
		for _, path := range maliciousPaths {
			err = client.Write(ctx, path, testData)
			if err == nil {
				t.Errorf("Write() with malicious path %s should have failed", path)
			}
		}
	})
	
	t.Run("home directory expansion", func(t *testing.T) {
		// Skip if we can't get home directory
		home, err := os.UserHomeDir()
		if err != nil {
			t.Skip("Cannot get home directory")
		}
		
		// Create client with ~ path
		client, err := NewLocalFSClient("~/parquet-test")
		if err != nil {
			t.Fatalf("NewLocalFSClient() error = %v", err)
		}
		defer func() {
			client.Close()
			// Clean up test directory
			os.RemoveAll(filepath.Join(home, "parquet-test"))
		}()
		
		// Verify path was expanded
		if !strings.Contains(client.basePath, home) {
			t.Errorf("Home directory not expanded: %s", client.basePath)
		}
	})
}

func TestRetryableStorageClient(t *testing.T) {
	t.Run("successful write", func(t *testing.T) {
		tmpDir := t.TempDir()
		baseClient, err := NewLocalFSClient(tmpDir)
		if err != nil {
			t.Fatalf("NewLocalFSClient() error = %v", err)
		}
		
		retryClient := NewRetryableStorageClient(baseClient, 3)
		defer retryClient.Close()
		
		ctx := context.Background()
		testData := []byte("test data")
		testKey := "test.parquet"
		
		err = retryClient.Write(ctx, testKey, testData)
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		
		// Verify file exists
		expectedPath := filepath.Join(tmpDir, testKey)
		if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
			t.Errorf("Expected file does not exist: %s", expectedPath)
		}
	})
	
	t.Run("context cancellation", func(t *testing.T) {
		// Use a mock client that checks context before writing
		mockClient := &mockStorageClient{
			writeFunc: func(ctx context.Context, key string, data []byte) error {
				// Check if context is already cancelled
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return nil
				}
			},
		}
		
		retryClient := NewRetryableStorageClient(mockClient, 3)
		defer retryClient.Close()
		
		// Create already cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		
		testData := []byte("test data")
		testKey := "test.parquet"
		
		err := retryClient.Write(ctx, testKey, testData)
		if err != context.Canceled {
			t.Errorf("Write() with cancelled context = %v, want context.Canceled", err)
		}
	})
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "context canceled",
			err:      context.Canceled,
			expected: false,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: false,
		},
		{
			name:     "generic error",
			err:      os.ErrNotExist,
			expected: true,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err)
			if result != tt.expected {
				t.Errorf("isRetryableError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// BenchmarkLocalFSWrite benchmarks local filesystem writes
func BenchmarkLocalFSWrite(b *testing.B) {
	tmpDir := b.TempDir()
	client, err := NewLocalFSClient(tmpDir)
	if err != nil {
		b.Fatalf("NewLocalFSClient() error = %v", err)
	}
	defer client.Close()
	
	ctx := context.Background()
	testData := make([]byte, 1024*1024) // 1MB of data
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	
	b.ResetTimer()
	b.SetBytes(int64(len(testData)))
	
	for i := 0; i < b.N; i++ {
		key := filepath.Join("bench", time.Now().Format("20060102-150405"), "test.parquet")
		if err := client.Write(ctx, key, testData); err != nil {
			b.Fatalf("Write() error = %v", err)
		}
	}
}

// mockStorageClient is a mock implementation for testing
type mockStorageClient struct {
	writeFunc func(ctx context.Context, key string, data []byte) error
	closeFunc func() error
}

func (m *mockStorageClient) Write(ctx context.Context, key string, data []byte) error {
	if m.writeFunc != nil {
		return m.writeFunc(ctx, key, data)
	}
	return nil
}

func (m *mockStorageClient) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}