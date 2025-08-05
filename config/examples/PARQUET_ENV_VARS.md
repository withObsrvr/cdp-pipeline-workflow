# Environment Variables for Parquet Archival Consumer

This document describes the environment variables used by the Parquet archival consumer for different storage backends.

## Google Cloud Storage (GCS)

### Required Environment Variables

**GOOGLE_APPLICATION_CREDENTIALS**
- Path to the service account JSON key file
- Example: `/path/to/service-account-key.json`
- Alternative: Use `gcloud auth application-default login` for local development

### Optional Environment Variables

**GOOGLE_CLOUD_PROJECT**
- GCP project ID (if not specified in credentials)
- Example: `my-project-123456`

### Example Setup
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/home/user/credentials/stellar-archive-sa.json"
```

## Amazon S3

### Required Environment Variables

**AWS_ACCESS_KEY_ID**
- AWS access key for authentication
- Example: `AKIAIOSFODNN7EXAMPLE`

**AWS_SECRET_ACCESS_KEY**
- AWS secret key for authentication
- Example: `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`

### Optional Environment Variables

**AWS_REGION**
- AWS region (can also be specified in config)
- Default: `us-east-1`
- Example: `us-west-2`

**AWS_SESSION_TOKEN**
- For temporary credentials (STS)
- Required when using assumed roles

**AWS_PROFILE**
- Use a specific AWS profile from ~/.aws/credentials
- Example: `stellar-archive`

### Example Setup
```bash
export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_REGION="us-west-2"
```

## Local Filesystem

No environment variables required for local filesystem storage.

## Common Pipeline Variables

These environment variables can be used in pipeline configurations:

**DATABASE_URL**
- PostgreSQL connection string for real-time consumers
- Example: `postgresql://user:pass@localhost:5432/stellar`

**REDIS_URL**
- Redis connection string for cache consumers
- Example: `redis://localhost:6379`

**ARCHIVE_BUCKET**
- Bucket name for archival storage
- Example: `stellar-data-lake`

## Using Environment Variables in Configuration

Environment variables can be referenced in YAML configurations using `${VAR_NAME}` syntax:

```yaml
consumers:
  - type: SaveToParquet
    config:
      storage_type: "GCS"
      bucket_name: ${ARCHIVE_BUCKET}
      path_prefix: ${ARCHIVE_PREFIX:-default-prefix}  # With default value
```

## IAM Permissions

### GCS Required Permissions
- `storage.buckets.get`
- `storage.objects.create`
- `storage.objects.delete` (for overwrites)

### S3 Required Permissions
- `s3:GetBucketLocation`
- `s3:PutObject`
- `s3:PutObjectAcl` (if using ACLs)

## Troubleshooting

### GCS Authentication Issues
1. Check if `GOOGLE_APPLICATION_CREDENTIALS` points to valid JSON file
2. Verify service account has necessary permissions
3. Try `gcloud auth application-default login` for local testing

### S3 Authentication Issues
1. Verify AWS credentials are not expired
2. Check if bucket region matches configuration
3. Use `aws sts get-caller-identity` to verify credentials

### General Issues
- Enable debug mode in consumer config: `debug: true`
- Check pipeline logs for detailed error messages
- Verify network connectivity to cloud providers