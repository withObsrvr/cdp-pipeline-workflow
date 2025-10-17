package consumer

import (
	"testing"
)

// TestValidateBucketNaming tests the bucket naming validation for hybrid model
func TestValidateBucketNaming(t *testing.T) {
	tests := []struct {
		name                string
		dataPath            string
		schemaName          string
		enablePublicArchive bool
		expectError         bool
		errorContains       string
	}{
		// Local paths (should pass)
		{
			name:                "Local path with public archive",
			dataPath:            "/tmp/ducklake_data",
			schemaName:          "stellarcarbon",
			enablePublicArchive: true,
			expectError:         false,
		},
		{
			name:                "Local path with private archive",
			dataPath:            "./ducklake_data",
			schemaName:          "main",
			enablePublicArchive: false,
			expectError:         false,
		},

		// Public archive paths (enable_public_archive: true)
		{
			name:                "Valid public archive bucket",
			dataPath:            "s3://obsrvr-public-archives/data",
			schemaName:          "stellarcarbon",
			enablePublicArchive: true,
			expectError:         false,
		},
		{
			name:                "Invalid public archive bucket (wrong name)",
			dataPath:            "s3://my-public-bucket/data",
			schemaName:          "stellarcarbon",
			enablePublicArchive: true,
			expectError:         true,
			errorContains:       "public archive must use 'obsrvr-public-archives' bucket",
		},
		{
			name:                "Invalid public archive bucket (using private bucket)",
			dataPath:            "s3://obsrvr-private-stellarcarbon/data",
			schemaName:          "stellarcarbon",
			enablePublicArchive: true,
			expectError:         true,
			errorContains:       "public archive must use 'obsrvr-public-archives' bucket",
		},

		// Private archive paths (enable_public_archive: false)
		{
			name:                "Valid private archive bucket (soroswap)",
			dataPath:            "s3://obsrvr-private-soroswap/data",
			schemaName:          "main",
			enablePublicArchive: false,
			expectError:         false,
		},
		{
			name:                "Valid private archive bucket (stellarcarbon)",
			dataPath:            "s3://obsrvr-private-stellarcarbon/data",
			schemaName:          "main",
			enablePublicArchive: false,
			expectError:         false,
		},
		{
			name:                "Invalid private archive bucket (missing prefix)",
			dataPath:            "s3://my-private-bucket/data",
			schemaName:          "main",
			enablePublicArchive: false,
			expectError:         true,
			errorContains:       "private customer bucket must follow 'obsrvr-private-{customer_name}' convention",
		},
		{
			name:                "Invalid private archive bucket (using public bucket)",
			dataPath:            "s3://obsrvr-public-archives/data",
			schemaName:          "main",
			enablePublicArchive: false,
			expectError:         true,
			errorContains:       "private customer bucket must follow 'obsrvr-private-{customer_name}' convention",
		},
		{
			name:                "Invalid private archive bucket (wrong prefix)",
			dataPath:            "s3://obsrvr-public-soroswap/data",
			schemaName:          "main",
			enablePublicArchive: false,
			expectError:         true,
			errorContains:       "private customer bucket must follow 'obsrvr-private-{customer_name}' convention",
		},

		// Edge cases
		{
			name:                "S3 path without trailing path",
			dataPath:            "s3://obsrvr-private-customer",
			schemaName:          "main",
			enablePublicArchive: false,
			expectError:         false,
		},
		{
			name:                "Invalid S3 path format (empty bucket)",
			dataPath:            "s3:///data",
			schemaName:          "main",
			enablePublicArchive: false,
			expectError:         true,
			errorContains:       "invalid S3 path format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBucketNaming(tt.dataPath, tt.schemaName, tt.enablePublicArchive)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorContains != "" {
					// Check if error message contains expected substring
					if len(err.Error()) < len(tt.errorContains) ||
					   !contains(err.Error(), tt.errorContains) {
						t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
					}
				}
			} else {
				if err != nil {
					t.Errorf("expected no error but got: %v", err)
				}
			}
		})
	}
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestNewSaveToDuckLakeWithS3ConfigValidation tests bucket naming validation only
// Note: We don't test full S3 connection because it requires real S3 buckets
func TestNewSaveToDuckLakeWithS3Config(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError bool
		errorMsg    string
	}{
		// Note: These tests validate bucket naming before attempting S3 connection
		// Full S3 integration tests would require actual S3 buckets or mocking
		{
			name: "Invalid public bucket name",
			config: map[string]interface{}{
				"catalog_path":          "ducklake:test.ducklake",
				"data_path":             "s3://wrong-bucket/data",
				"catalog_name":          "test_lake",
				"schema_name":           "stellarcarbon",
				"enable_public_archive": true,
				"batch_size":            float64(100),
			},
			expectError: true,
			errorMsg:    "bucket naming validation failed",
		},
		{
			name: "Invalid private bucket name",
			config: map[string]interface{}{
				"catalog_path":          "ducklake:test.ducklake",
				"data_path":             "s3://random-bucket/data",
				"catalog_name":          "test_lake",
				"schema_name":           "main",
				"enable_public_archive": false,
				"batch_size":            float64(100),
			},
			expectError: true,
			errorMsg:    "bucket naming validation failed",
		},
		{
			name: "Local path (no validation)",
			config: map[string]interface{}{
				"catalog_path": "ducklake:/tmp/test.ducklake",
				"data_path":    "/tmp/ducklake_data",
				"catalog_name": "test_lake",
				"schema_name":  "main",
				"batch_size":   float64(100),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc, err := NewSaveToDuckLake(tt.config)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorMsg != "" && !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("expected no error but got: %v", err)
				} else {
					// Clean up successfully created consumer
					if consumer, ok := proc.(*SaveToDuckLake); ok {
						consumer.Close()
					}
				}
			}
		})
	}
}
