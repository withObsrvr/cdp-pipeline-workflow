// Package bronze provides components for reading Bronze layer parquet files
// produced by the Bronze Copier.
package bronze

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/stellar/go/xdr"
)

// ParquetReader reads ledgers from Bronze layer parquet files.
// It uses DuckDB to query parquet files and unmarshal XDR bytes
// to xdr.LedgerCloseMeta using the Stellar official XDR library.
type ParquetReader struct {
	connector *duckdb.Connector
	db        *sql.DB
}

// NewParquetReader creates a new Bronze parquet reader.
// The reader uses an in-memory DuckDB instance to query parquet files.
func NewParquetReader() (*ParquetReader, error) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, fmt.Errorf("create duckdb connector: %w", err)
	}

	db := sql.OpenDB(connector)

	return &ParquetReader{
		connector: connector,
		db:        db,
	}, nil
}

// Close closes the database connection.
func (r *ParquetReader) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

// ConfigureGCS configures the reader for GCS access using Application Default Credentials.
// This uses the native GCS extension which supports credential chain and requester pays buckets.
func (r *ParquetReader) ConfigureGCS(ctx context.Context) error {
	// Install and load the native GCS extension from community repository
	// This extension uses Google's native API (not S3 compatibility) and handles:
	// - Application Default Credentials (ADC)
	// - Requester pays buckets
	// - Hive partition paths with '=' characters
	_, err := r.db.ExecContext(ctx, "INSTALL gcs FROM community; LOAD gcs;")
	if err != nil {
		return fmt.Errorf("install gcs extension: %w", err)
	}

	// Create a GCP secret using credential chain (uses ADC)
	_, err = r.db.ExecContext(ctx, `
		CREATE SECRET IF NOT EXISTS gcs_secret (
			TYPE GCP,
			PROVIDER CREDENTIAL_CHAIN
		)
	`)
	if err != nil {
		return fmt.Errorf("create gcs secret: %w", err)
	}

	log.Printf("BronzeParquetReader: Configured for GCS access (using native GCS extension with credential chain)")
	return nil
}

// ConfigureGCSWithHMAC configures the reader for GCS access with HMAC credentials.
// HMAC keys can be created in the GCP Console under Cloud Storage > Settings > Interoperability.
func (r *ParquetReader) ConfigureGCSWithHMAC(ctx context.Context, accessKeyID, secretAccessKey string) error {
	// Install and load httpfs extension for cloud storage access
	_, err := r.db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;")
	if err != nil {
		return fmt.Errorf("install httpfs: %w", err)
	}

	// Create a GCS secret with HMAC credentials
	query := fmt.Sprintf(`
		CREATE SECRET IF NOT EXISTS gcs_secret (
			TYPE GCS,
			KEY_ID '%s',
			SECRET '%s'
		)
	`, accessKeyID, secretAccessKey)

	_, err = r.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create gcs secret: %w", err)
	}

	log.Printf("BronzeParquetReader: Configured for GCS access (using HMAC)")
	return nil
}

// ConfigureS3 configures the reader for S3 access with explicit credentials.
func (r *ParquetReader) ConfigureS3(ctx context.Context, accessKeyID, secretAccessKey, region, endpoint string) error {
	// Install and load httpfs extension
	_, err := r.db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;")
	if err != nil {
		return fmt.Errorf("install httpfs: %w", err)
	}

	// Configure S3 credentials with optional custom endpoint
	if accessKeyID != "" && secretAccessKey != "" {
		var query string
		if endpoint != "" {
			// For S3-compatible storage (MinIO, B2, etc.), include endpoint in secret
			// Strip https:// prefix if present - DuckDB expects just the host
			endpointHost := endpoint
			if len(endpoint) > 8 && endpoint[:8] == "https://" {
				endpointHost = endpoint[8:]
			} else if len(endpoint) > 7 && endpoint[:7] == "http://" {
				endpointHost = endpoint[7:]
			}

			query = fmt.Sprintf(`
				CREATE SECRET (
					TYPE S3,
					KEY_ID '%s',
					SECRET '%s',
					REGION '%s',
					ENDPOINT '%s',
					URL_STYLE 'path'
				)
			`, accessKeyID, secretAccessKey, region, endpointHost)
		} else {
			query = fmt.Sprintf(`
				CREATE SECRET (
					TYPE S3,
					KEY_ID '%s',
					SECRET '%s',
					REGION '%s'
				)
			`, accessKeyID, secretAccessKey, region)
		}

		_, err = r.db.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("configure s3 credentials: %w", err)
		}
	}

	log.Printf("BronzeParquetReader: Configured for S3 access (region=%s)", region)
	return nil
}

// LedgerResult represents a ledger read from a parquet file.
type LedgerResult struct {
	Sequence        uint64
	LedgerCloseMeta xdr.LedgerCloseMeta
}

// ReadLedgers reads ledgers from a parquet file and returns them via channels.
// The parquetPath can be a local path or a cloud storage URL (gs://, s3://).
// Ledgers are filtered to the specified range [startLedger, endLedger].
// If endLedger is 0, all ledgers >= startLedger are returned.
//
// Returns two channels:
// - ledgerCh: receives xdr.LedgerCloseMeta for each ledger
// - errCh: receives any error that occurs (closes after completion or error)
func (r *ParquetReader) ReadLedgers(ctx context.Context, parquetPath string, startLedger, endLedger uint32) (<-chan xdr.LedgerCloseMeta, <-chan error) {
	ledgerCh := make(chan xdr.LedgerCloseMeta, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(ledgerCh)
		defer close(errCh)

		// Build query with ledger range filter
		var query string
		if endLedger > 0 {
			query = fmt.Sprintf(`
				SELECT ledger_sequence, xdr_bytes
				FROM read_parquet('%s')
				WHERE ledger_sequence >= %d AND ledger_sequence <= %d
				ORDER BY ledger_sequence
			`, parquetPath, startLedger, endLedger)
		} else {
			query = fmt.Sprintf(`
				SELECT ledger_sequence, xdr_bytes
				FROM read_parquet('%s')
				WHERE ledger_sequence >= %d
				ORDER BY ledger_sequence
			`, parquetPath, startLedger)
		}

		rows, err := r.db.QueryContext(ctx, query)
		if err != nil {
			errCh <- fmt.Errorf("query parquet %s: %w", parquetPath, err)
			return
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var seq uint64
			var xdrBytes []byte

			if err := rows.Scan(&seq, &xdrBytes); err != nil {
				errCh <- fmt.Errorf("scan row: %w", err)
				return
			}

			// Unmarshal XDR bytes to LedgerCloseMeta using Stellar official library
			var lcm xdr.LedgerCloseMeta
			if err := lcm.UnmarshalBinary(xdrBytes); err != nil {
				errCh <- fmt.Errorf("unmarshal ledger %d: %w", seq, err)
				return
			}

			// Verify sequence matches
			if uint32(seq) != lcm.LedgerSequence() {
				errCh <- fmt.Errorf("ledger sequence mismatch: parquet has %d, XDR has %d", seq, lcm.LedgerSequence())
				return
			}

			select {
			case ledgerCh <- lcm:
				count++
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}

		if err := rows.Err(); err != nil {
			errCh <- fmt.Errorf("rows iteration: %w", err)
			return
		}

		log.Printf("BronzeParquetReader: Read %d ledgers from %s", count, parquetPath)
	}()

	return ledgerCh, errCh
}

// ReadLedgersSync reads ledgers synchronously and returns a slice.
// This is a convenience method for testing and small datasets.
// For large datasets, use ReadLedgers with channels.
func (r *ParquetReader) ReadLedgersSync(ctx context.Context, parquetPath string, startLedger, endLedger uint32) ([]xdr.LedgerCloseMeta, error) {
	ledgerCh, errCh := r.ReadLedgers(ctx, parquetPath, startLedger, endLedger)

	var ledgers []xdr.LedgerCloseMeta
	for lcm := range ledgerCh {
		ledgers = append(ledgers, lcm)
	}

	// Check for errors
	select {
	case err := <-errCh:
		if err != nil {
			return nil, err
		}
	default:
	}

	return ledgers, nil
}

// GetParquetInfo returns information about a parquet file.
type ParquetInfo struct {
	RowCount      int64
	MinLedger     uint64
	MaxLedger     uint64
	BronzeVersion string
	EraID         string
	Network       string
}

// GetParquetInfo retrieves metadata about a parquet file.
func (r *ParquetReader) GetParquetInfo(ctx context.Context, parquetPath string) (*ParquetInfo, error) {
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) as row_count,
			MIN(ledger_sequence) as min_ledger,
			MAX(ledger_sequence) as max_ledger,
			ANY_VALUE(bronze_version) as bronze_version,
			ANY_VALUE(era_id) as era_id,
			ANY_VALUE(network) as network
		FROM read_parquet('%s')
	`, parquetPath)

	row := r.db.QueryRowContext(ctx, query)

	var info ParquetInfo
	err := row.Scan(
		&info.RowCount,
		&info.MinLedger,
		&info.MaxLedger,
		&info.BronzeVersion,
		&info.EraID,
		&info.Network,
	)
	if err != nil {
		return nil, fmt.Errorf("query parquet info: %w", err)
	}

	return &info, nil
}

// Manifest represents the Bronze Copier partition manifest.
// This supports both legacy single-file format and new multi-file format.
type Manifest struct {
	// Legacy fields (single file per partition)
	LedgerStart   int    `json:"ledger_start"`
	LedgerEnd     int    `json:"ledger_end"`
	Network       string `json:"network"`
	EraID         string `json:"era_id"`
	BronzeVersion string `json:"bronze_version"`
	RowCount      int    `json:"row_count"`

	// Multi-file partition fields
	Tables map[string]TableManifest `json:"tables,omitempty"`
}

// TableManifest describes a table's files within a partition.
type TableManifest struct {
	Files []FileManifest `json:"files"`
}

// FileManifest describes a single parquet file within a partition.
type FileManifest struct {
	File        string `json:"file"`
	LedgerStart int    `json:"ledger_start"`
	LedgerEnd   int    `json:"ledger_end"`
	RowCount    int    `json:"row_count"`
}

// IsMultiFile returns true if this manifest uses multi-file format.
func (m *Manifest) IsMultiFile() bool {
	return m.Tables != nil && len(m.Tables) > 0
}

// GetLedgerFiles returns all parquet file names for ledgers_lcm_raw table,
// filtered to files that overlap with the specified ledger range.
// Returns nil if this is a legacy single-file manifest.
func (m *Manifest) GetLedgerFiles(startLedger, endLedger uint32) []FileManifest {
	if !m.IsMultiFile() {
		return nil
	}

	tableManifest, ok := m.Tables["ledgers_lcm_raw"]
	if !ok {
		return nil
	}

	var files []FileManifest
	for _, f := range tableManifest.Files {
		// Check if file overlaps with requested range
		if endLedger > 0 && uint32(f.LedgerStart) > endLedger {
			continue
		}
		if uint32(f.LedgerEnd) < startLedger {
			continue
		}
		files = append(files, f)
	}

	return files
}

// ReadManifest reads a manifest file from the given path.
// Supports local files, GCS (gs://), and S3 (s3://) paths.
func ReadManifest(ctx context.Context, manifestPath string) (*Manifest, error) {
	var data []byte
	var err error

	switch {
	case strings.HasPrefix(manifestPath, "gs://"):
		data, err = readGCSFile(ctx, manifestPath)
	case strings.HasPrefix(manifestPath, "s3://"):
		data, err = readS3File(ctx, manifestPath)
	default:
		data, err = os.ReadFile(manifestPath)
	}

	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", manifestPath, err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", manifestPath, err)
	}

	return &manifest, nil
}

// readGCSFile reads a file from GCS using the native Go client.
// This uses Application Default Credentials.
func readGCSFile(ctx context.Context, gcsPath string) ([]byte, error) {
	// Parse gs://bucket/path
	if !strings.HasPrefix(gcsPath, "gs://") {
		return nil, fmt.Errorf("invalid GCS path: %s", gcsPath)
	}

	path := strings.TrimPrefix(gcsPath, "gs://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid GCS path: %s", gcsPath)
	}

	bucket := parts[0]
	object := parts[1]

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create GCS client: %w", err)
	}
	defer client.Close()

	// Set requester pays bucket option
	bkt := client.Bucket(bucket).UserProject("obsrvr-stellar-ledger-data")
	reader, err := bkt.Object(object).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("open GCS object %s: %w", gcsPath, err)
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// readS3File reads a file from S3 using HTTPS.
// Note: This requires the S3 bucket to be publicly accessible or
// uses environment variables AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY.
func readS3File(ctx context.Context, s3Path string) ([]byte, error) {
	// For S3, we'll use a simple HTTP request with the S3 URL
	// This works for public buckets. For private buckets, we'd need AWS SDK.
	// Parse s3://bucket/path
	if !strings.HasPrefix(s3Path, "s3://") {
		return nil, fmt.Errorf("invalid S3 path: %s", s3Path)
	}

	path := strings.TrimPrefix(s3Path, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid S3 path: %s", s3Path)
	}

	bucket := parts[0]
	object := parts[1]

	// Try to use virtual-hosted style URL first
	url := fmt.Sprintf("https://%s.s3.amazonaws.com/%s", bucket, object)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch S3 object: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("S3 returned status %d for %s", resp.StatusCode, s3Path)
	}

	return io.ReadAll(resp.Body)
}
