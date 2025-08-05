package consumer

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// createStorageClient creates the appropriate storage client based on config
func createStorageClient(cfg SaveToParquetConfig) (StorageClient, error) {
	switch cfg.StorageType {
	case "FS":
		return NewLocalFSClient(cfg.LocalPath)
	case "GCS":
		return NewGCSClient(cfg.BucketName)
	case "S3":
		return NewS3Client(cfg.BucketName, cfg.Region)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.StorageType)
	}
}

// LocalFSClient implements StorageClient for local filesystem
type LocalFSClient struct {
	basePath string
}

// NewLocalFSClient creates a new local filesystem storage client
func NewLocalFSClient(basePath string) (*LocalFSClient, error) {
	// Expand home directory if needed
	if basePath == "~" || len(basePath) > 1 && basePath[:2] == "~/" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get home directory: %w", err)
		}
		basePath = filepath.Join(home, basePath[2:])
	}
	
	// Convert to absolute path
	absPath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}
	
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(absPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}
	
	log.Printf("LocalFSClient initialized with path: %s", absPath)
	
	return &LocalFSClient{
		basePath: absPath,
	}, nil
}

// Write implements atomic file write for local filesystem
func (c *LocalFSClient) Write(ctx context.Context, key string, data []byte) error {
	// Sanitize the key to prevent directory traversal
	cleanKey := filepath.Clean(key)
	if filepath.IsAbs(cleanKey) {
		return fmt.Errorf("absolute paths not allowed in key: %s", key)
	}
	
	fullPath := filepath.Join(c.basePath, cleanKey)
	
	// Ensure the file is within basePath
	// Check if the full path starts with the base path
	rel, err := filepath.Rel(c.basePath, fullPath)
	if err != nil || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("invalid key path: %s", key)
	}
	
	// Create directory structure
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}
	
	// Write to temporary file first for atomic operation
	tmpFile := fmt.Sprintf("%s.tmp.%d", fullPath, time.Now().UnixNano())
	
	// Write data to temporary file
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}
	
	// Ensure data is flushed to disk
	if file, err := os.Open(tmpFile); err == nil {
		file.Sync()
		file.Close()
	}
	
	// Atomic rename
	if err := os.Rename(tmpFile, fullPath); err != nil {
		// Clean up temporary file on failure
		os.Remove(tmpFile)
		return fmt.Errorf("failed to rename file: %w", err)
	}
	
	log.Printf("LocalFSClient: Successfully wrote %d bytes to %s", len(data), fullPath)
	return nil
}

// Close implements StorageClient interface
func (c *LocalFSClient) Close() error {
	// Nothing to close for local filesystem
	return nil
}

// GCSClient implements StorageClient for Google Cloud Storage
type GCSClient struct {
	client   *storage.Client
	bucket   string
	metadata map[string]string
}

// NewGCSClient creates a new Google Cloud Storage client
func NewGCSClient(bucketName string) (*GCSClient, error) {
	ctx := context.Background()
	
	// Create GCS client using default credentials
	// Credentials can be set via:
	// 1. GOOGLE_APPLICATION_CREDENTIALS environment variable
	// 2. gcloud auth application-default login
	// 3. GCE metadata service
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	
	// Verify bucket exists and is accessible
	bucket := client.Bucket(bucketName)
	_, err = bucket.Attrs(ctx)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to access bucket %s: %w", bucketName, err)
	}
	
	log.Printf("GCSClient initialized for bucket: %s", bucketName)
	
	return &GCSClient{
		client: client,
		bucket: bucketName,
		metadata: map[string]string{
			"format":    "parquet",
			"generator": "cdp-pipeline-workflow",
			"version":   "1.0",
		},
	}, nil
}

// Write implements StorageClient interface for GCS
func (c *GCSClient) Write(ctx context.Context, key string, data []byte) error {
	bucket := c.client.Bucket(c.bucket)
	obj := bucket.Object(key)
	
	// Create writer with context and metadata
	w := obj.NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	w.Metadata = c.metadata
	
	// Add cache control for better performance
	w.CacheControl = "no-cache, max-age=0"
	
	// Write data
	if _, err := w.Write(data); err != nil {
		w.Close()
		return fmt.Errorf("failed to write to GCS object %s: %w", key, err)
	}
	
	// Close writer to finalize upload
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer for %s: %w", key, err)
	}
	
	return nil
}

// Close implements StorageClient interface
func (c *GCSClient) Close() error {
	return c.client.Close()
}

// S3Client implements StorageClient for Amazon S3
type S3Client struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
	metadata map[string]string
}

// NewS3Client creates a new Amazon S3 client
func NewS3Client(bucketName, region string) (*S3Client, error) {
	ctx := context.Background()
	
	// Load AWS configuration
	// Credentials can be set via:
	// 1. AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
	// 2. ~/.aws/credentials file
	// 3. IAM role (when running on EC2/ECS/Lambda)
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithRetryMode(aws.RetryModeStandard),
		config.WithRetryMaxAttempts(3),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	
	// Create S3 client
	client := s3.NewFromConfig(cfg)
	
	// Verify bucket exists and is accessible
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to access bucket %s: %w", bucketName, err)
	}
	
	// Create uploader for better performance with large files
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = 10 * 1024 * 1024 // 10MB per part
		u.Concurrency = 3              // 3 concurrent uploads
	})
	
	log.Printf("S3Client initialized for bucket: %s in region: %s", bucketName, region)
	
	return &S3Client{
		client:   client,
		uploader: uploader,
		bucket:   bucketName,
		metadata: map[string]string{
			"format":    "parquet",
			"generator": "cdp-pipeline-workflow",
			"version":   "1.0",
		},
	}, nil
}

// Write implements StorageClient interface for S3
func (c *S3Client) Write(ctx context.Context, key string, data []byte) error {
	// Use the uploader for better performance
	_, err := c.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
		Metadata:    c.metadata,
		// Server-side encryption (optional, uncomment if needed)
		// ServerSideEncryption: types.ServerSideEncryptionAes256,
		// Storage class for cost optimization (optional)
		StorageClass: types.StorageClassStandard,
	})
	
	if err != nil {
		return fmt.Errorf("failed to upload to S3 %s/%s: %w", c.bucket, key, err)
	}
	
	return nil
}

// Close implements StorageClient interface
func (c *S3Client) Close() error {
	// S3 client doesn't need explicit closing
	return nil
}

// RetryableStorageClient wraps a StorageClient with retry logic
type RetryableStorageClient struct {
	client      StorageClient
	maxRetries  int
	retryDelay  time.Duration
}

// NewRetryableStorageClient creates a storage client with retry capabilities
func NewRetryableStorageClient(client StorageClient, maxRetries int) *RetryableStorageClient {
	return &RetryableStorageClient{
		client:     client,
		maxRetries: maxRetries,
		retryDelay: time.Second,
	}
}

// Write implements StorageClient with retry logic
func (r *RetryableStorageClient) Write(ctx context.Context, key string, data []byte) error {
	var lastErr error
	
	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			delay := r.retryDelay * time.Duration(1<<(attempt-1))
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
			
			log.Printf("Retrying write after %v (attempt %d/%d)", delay, attempt, r.maxRetries)
			
			select {
			case <-time.After(delay):
				// Continue with retry
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		
		err := r.client.Write(ctx, key, data)
		if err == nil {
			return nil
		}
		
		lastErr = err
		
		// Don't retry on certain errors
		if !isRetryableError(err) {
			return err
		}
	}
	
	return fmt.Errorf("failed after %d retries: %w", r.maxRetries, lastErr)
}

// Close implements StorageClient interface
func (r *RetryableStorageClient) Close() error {
	return r.client.Close()
}

// isRetryableError determines if an error should trigger a retry
func isRetryableError(err error) bool {
	// Add specific error checks here
	// For now, retry on all errors except context cancellation
	if err == context.Canceled || err == context.DeadlineExceeded {
		return false
	}
	return true
}