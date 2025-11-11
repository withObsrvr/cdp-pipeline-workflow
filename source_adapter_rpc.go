package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go/xdr"
	cdpProcessor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// =============================================================
// Configuration and Type Definitions
// =============================================================

// SorobanConfig holds the configuration for the RPC adapter.
type SorobanConfig struct {
	RPCURL       string
	AuthHeader   string
	StartLedger  uint64
	BatchSize    int
	PollInterval time.Duration

	// Extra holds any additional config values (like rpc_method and its arguments).
	Extra map[string]interface{}
}

// RPCError represents an error returned by the RPC server.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	return e.Message
}

// EventStats holds statistics about processed events.
type EventStats struct {
	ProcessedEvents  uint64
	SuccessfulEvents uint64
	FailedEvents     uint64
	LastLedger       uint64
	LastEventTime    time.Time
}

// PaginationOptions is used for paginating results in getEvents.
type PaginationOptions struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit"`
}

// GetEventsResponse defines the response for the getEvents RPC call.
type GetEventsResponse struct {
	Result struct {
		Events       []cdpProcessor.Event `json:"events"`
		LatestLedger uint64               `json:"latestLedger"`
	} `json:"result"`
}

// GetLatestLedgerResponse defines the response for getLatestLedger.
type GetLatestLedgerResponse struct {
	Result struct {
		Sequence uint64 `json:"sequence"`
	} `json:"result"`
}

// FeeStatsResponse defines the response for getFeeStats.
type FeeStatsResponse struct {
	Result struct {
		AverageFee float64 `json:"average_fee"`
		// Additional metrics can be added here.
	} `json:"result"`
}

// HealthResponse defines the response for getHealth.
type HealthResponse struct {
	Result string `json:"result"` // e.g., "healthy"
}

// LedgerEntriesResponse defines the response for getLedgerEntries.
type LedgerEntriesResponse struct {
	Result interface{} `json:"result"`
}

// LedgersResponse defines the response for getLedgers.
type LedgersResponse struct {
	Result *LedgersResult `json:"result"`
}

// LedgersResult contains the ledgers array and latest ledger sequence.
type LedgersResult struct {
	Ledgers      []interface{} `json:"ledgers"`       // Array of ledger objects
	LatestLedger uint64        `json:"latestLedger"`  // Latest known ledger sequence
}

// NetworkResponse defines the response for getNetwork.
type NetworkResponse struct {
	Result string `json:"result"`
}

// TransactionResponse defines the response for getTransaction.
type TransactionResponse struct {
	Result interface{} `json:"result"`
}

// TransactionsResponse defines the response for getTransactions.
type TransactionsResponse struct {
	Result interface{} `json:"result"`
}

// VersionInfoResponse defines the response for getVersionInfo.
type VersionInfoResponse struct {
	Result struct {
		Version string `json:"version"`
	} `json:"result"`
}

// SendTransactionResponse defines the response for sendTransaction.
type SendTransactionResponse struct {
	Result struct {
		Hash string `json:"hash"`
		// Additional fields can be added here.
	} `json:"result"`
}

// SimulateTransactionResponse defines the response for simulateTransaction.
type SimulateTransactionResponse struct {
	Result interface{} `json:"result"`
}

// Checkpoint stores the state of ledger processing for crash recovery.
type Checkpoint struct {
	LastProcessedLedger uint64    `json:"last_processed_ledger"`
	Timestamp           time.Time `json:"timestamp"`
	TotalProcessed      uint64    `json:"total_processed"`
	PipelineName        string    `json:"pipeline_name"`
}

// RPCRequest is a generic JSON-RPC request structure.
type RPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

// EventFilter represents a filter used when querying events.
type EventFilter struct {
	Type        string     `json:"type"`
	ContractIds []string   `json:"contractIds,omitempty"`
	Topics      [][]string `json:"topics,omitempty"`
}

// GetEventsRequestParams defines the parameters for the getEvents call.
type GetEventsRequestParams struct {
	StartLedger uint64            `json:"startLedger,omitempty"` // Omit if using a cursor.
	EndLedger   uint64            `json:"endLedger,omitempty"`   // Optional end ledger (exclusive).
	Filters     []EventFilter     `json:"filters"`               // Required.
	Pagination  PaginationOptions `json:"pagination,omitempty"`  // Supports "cursor" and "limit".
	XDRFormat   string            `json:"xdrFormat,omitempty"`   // Either "json" or "xdr" as required.
}

// =============================================================
// SorobanSourceAdapter Structure and Constructor
// =============================================================

// SorobanSourceAdapter ingests events/queries data from the Stellar-RPC endpoint.
type SorobanSourceAdapter struct {
	config     SorobanConfig
	processors []cdpProcessor.Processor
	client     *http.Client
	lastLedger uint64
	stats      EventStats
}

// NewSorobanSourceAdapter creates a new RPC source adapter using a configuration format
// similar to our other adapters. Optional keys include "start_ledger", "batch_size", and "poll_interval".
func NewSorobanSourceAdapter(config map[string]interface{}) (SourceAdapter, error) {
	log.Printf("Initializing SorobanSourceAdapter with config: %+v", config)

	// Extract required parameters.
	rpcURL, ok := config["rpc_url"].(string)
	if !ok {
		return nil, errors.New("rpc_url must be specified")
	}
	authHeader, ok := config["auth_header"].(string)
	if !ok {
		return nil, errors.New("auth_header must be specified")
	}
	batchSize := 100
	if size, ok := config["batch_size"].(int); ok {
		batchSize = size
	}
	pollInterval := 10 * time.Second
	if pi, ok := config["poll_interval"].(int); ok {
		pollInterval = time.Duration(pi) * time.Second
	}

	// Build Extra keys (remove standard ones)
	extra := make(map[string]interface{})
	for k, v := range config {
		if k != "rpc_url" && k != "auth_header" && k != "batch_size" && k != "poll_interval" && k != "start_ledger" {
			extra[k] = v
		}
	}

	var startLedger uint64 = 0
	if sl, ok := config["start_ledger"].(int); ok {
		startLedger = uint64(sl)
	} else if slFloat, ok := config["start_ledger"].(float64); ok {
		startLedger = uint64(slFloat)
	}

	adapter := &SorobanSourceAdapter{
		config: SorobanConfig{
			RPCURL:       rpcURL,
			AuthHeader:   authHeader,
			BatchSize:    batchSize,
			PollInterval: pollInterval,
			StartLedger:  startLedger,
			Extra:        extra,
		},
		client:     &http.Client{Timeout: 30 * time.Second},
		processors: []cdpProcessor.Processor{},
	}

	// Initialize ledger information.
	if adapter.config.StartLedger != 0 {
		adapter.lastLedger = adapter.config.StartLedger
		adapter.stats.LastLedger = adapter.config.StartLedger
		adapter.stats.LastEventTime = time.Now()
		log.Printf("Initialized SorobanSourceAdapter starting from configured ledger: %d", adapter.lastLedger)
	} else {
		latestLedger, err := adapter.GetLatestLedger(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to get latest ledger: %w", err)
		}
		adapter.lastLedger = latestLedger
		adapter.stats.LastLedger = latestLedger
		adapter.stats.LastEventTime = time.Now()
		log.Printf("Initialized SorobanSourceAdapter starting from latest ledger: %d", adapter.lastLedger)
	}

	return adapter, nil
}

// =============================================================
// Generic RPC Call Support
// =============================================================

// CallRPC makes a JSON-RPC call to the Stellar-RPC server using the specified method and parameters.
// The response is decoded into the provided result.
func (s *SorobanSourceAdapter) CallRPC(ctx context.Context, method string, params interface{}, result interface{}) error {
	reqPayload := RPCRequest{
		JSONRPC: "2.0",
		ID:      1, // In production you might want to generate unique IDs.
		Method:  method,
		Params:  params,
	}

	return s.sendRPCRequest(ctx, reqPayload, result)
}

// sendRPCRequest performs the HTTP POST to the RPC endpoint, logs the request/response, and decodes the JSON response.
func (s *SorobanSourceAdapter) sendRPCRequest(ctx context.Context, req interface{}, resp interface{}) error {
	jsonData, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "failed to marshal request")
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", s.config.RPCURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", s.config.AuthHeader)

	httpResp, err := s.client.Do(httpReq)
	if err != nil {
		return errors.Wrap(err, "failed to send request")
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := httputil.DumpResponse(httpResp, true)
		return fmt.Errorf("unexpected status code %d: %s", httpResp.StatusCode, string(body))
	}

	// Decode response
	if err := json.NewDecoder(httpResp.Body).Decode(resp); err != nil {
		return errors.Wrap(err, "failed to decode response")
	}

	return nil
}

// =============================================================
// RPC Method Helpers
// =============================================================

// GetEvents calls the getEvents RPC method.
func (s *SorobanSourceAdapter) GetEvents(
	ctx context.Context,
	startLedger, endLedger uint64,
	filters []EventFilter,
	pagination PaginationOptions,
	xdrFormat string,
) (*GetEventsResponse, error) {
	params := GetEventsRequestParams{
		StartLedger: startLedger,
		EndLedger:   endLedger,
		Filters:     filters,
		Pagination:  pagination,
		XDRFormat:   xdrFormat,
	}
	var resp GetEventsResponse
	if err := s.CallRPC(ctx, "getEvents", params, &resp); err != nil {
		return nil, fmt.Errorf("getEvents call failed: %w", err)
	}
	return &resp, nil
}

// GetFeeStats calls the getFeeStats RPC method.
func (s *SorobanSourceAdapter) GetFeeStats(ctx context.Context) (*FeeStatsResponse, error) {
	var resp FeeStatsResponse
	if err := s.CallRPC(ctx, "getFeeStats", nil, &resp); err != nil {
		return nil, fmt.Errorf("getFeeStats call failed: %w", err)
	}
	return &resp, nil
}

// GetHealth calls the getHealth RPC method.
func (s *SorobanSourceAdapter) GetHealth(ctx context.Context) (*HealthResponse, error) {
	var resp HealthResponse
	if err := s.CallRPC(ctx, "getHealth", nil, &resp); err != nil {
		return nil, fmt.Errorf("getHealth call failed: %w", err)
	}
	return &resp, nil
}

// GetLatestLedger calls the getLatestLedger RPC method.
func (s *SorobanSourceAdapter) GetLatestLedger(ctx context.Context) (uint64, error) {
	var resp GetLatestLedgerResponse
	if err := s.CallRPC(ctx, "getLatestLedger", nil, &resp); err != nil {
		return 0, fmt.Errorf("getLatestLedger call failed: %w", err)
	}
	return resp.Result.Sequence, nil
}

// GetLedgerEntries calls the getLedgerEntries RPC method.
func (s *SorobanSourceAdapter) GetLedgerEntries(ctx context.Context, ledger uint64) (*LedgerEntriesResponse, error) {
	params := map[string]interface{}{
		"ledger": ledger,
	}
	var resp LedgerEntriesResponse
	if err := s.CallRPC(ctx, "getLedgerEntries", params, &resp); err != nil {
		return nil, fmt.Errorf("getLedgerEntries call failed: %w", err)
	}
	return &resp, nil
}

// GetLedgers calls the getLedgers RPC method.
// Range semantics: [from, to) - from is inclusive, to is exclusive
// Example: GetLedgers(100, 110) fetches ledgers 100, 101, ..., 109 (10 ledgers)
func (s *SorobanSourceAdapter) GetLedgers(ctx context.Context, from uint64, to uint64) (*LedgersResponse, error) {
	// Validate range
	if to < from {
		return nil, fmt.Errorf("invalid range: to (%d) must be >= from (%d)", to, from)
	}

	// Calculate limit as the number of ledgers in the range [from, to)
	limit := to - from
	if limit == 0 {
		limit = 1 // At minimum, fetch the 'from' ledger
	}

	params := map[string]interface{}{
		"startLedger": from,
		"pagination": map[string]interface{}{
			"limit": limit,
		},
	}
	var resp LedgersResponse
	if err := s.CallRPC(ctx, "getLedgers", params, &resp); err != nil {
		return nil, fmt.Errorf("getLedgers call failed: %w", err)
	}
	return &resp, nil
}

// GetNetwork calls the getNetwork RPC method.
func (s *SorobanSourceAdapter) GetNetwork(ctx context.Context) (*NetworkResponse, error) {
	var resp NetworkResponse
	if err := s.CallRPC(ctx, "getNetwork", nil, &resp); err != nil {
		return nil, fmt.Errorf("getNetwork call failed: %w", err)
	}
	return &resp, nil
}

// GetTransaction calls the getTransaction RPC method.
func (s *SorobanSourceAdapter) GetTransaction(ctx context.Context, txHash string) (*TransactionResponse, error) {
	params := map[string]interface{}{
		"transaction": txHash,
	}
	var resp TransactionResponse
	if err := s.CallRPC(ctx, "getTransaction", params, &resp); err != nil {
		return nil, fmt.Errorf("getTransaction call failed: %w", err)
	}
	return &resp, nil
}

// GetTransactions calls the getTransactions RPC method.
func (s *SorobanSourceAdapter) GetTransactions(ctx context.Context, params map[string]interface{}) (*TransactionsResponse, error) {
	var resp TransactionsResponse
	if err := s.CallRPC(ctx, "getTransactions", params, &resp); err != nil {
		return nil, fmt.Errorf("getTransactions call failed: %w", err)
	}
	return &resp, nil
}

// GetVersionInfo calls the getVersionInfo RPC method.
func (s *SorobanSourceAdapter) GetVersionInfo(ctx context.Context) (*VersionInfoResponse, error) {
	var resp VersionInfoResponse
	if err := s.CallRPC(ctx, "getVersionInfo", nil, &resp); err != nil {
		return nil, fmt.Errorf("getVersionInfo call failed: %w", err)
	}
	return &resp, nil
}

// SendTransaction calls the sendTransaction RPC method.
func (s *SorobanSourceAdapter) SendTransaction(ctx context.Context, txData interface{}) (*SendTransactionResponse, error) {
	params := map[string]interface{}{
		"transaction": txData,
	}
	var resp SendTransactionResponse
	if err := s.CallRPC(ctx, "sendTransaction", params, &resp); err != nil {
		return nil, fmt.Errorf("sendTransaction call failed: %w", err)
	}
	return &resp, nil
}

// SimulateTransaction calls the simulateTransaction RPC method.
func (s *SorobanSourceAdapter) SimulateTransaction(ctx context.Context, txData interface{}) (*SimulateTransactionResponse, error) {
	params := map[string]interface{}{
		"transaction": txData,
	}
	var resp SimulateTransactionResponse
	if err := s.CallRPC(ctx, "simulateTransaction", params, &resp); err != nil {
		return nil, fmt.Errorf("simulateTransaction call failed: %w", err)
	}
	return &resp, nil
}

// =============================================================
// SourceAdapter Interface Definition
// =============================================================

// Run starts the adapter; here we check for an optional "rpc_method" config
// and call the matching RPC method.
func (s *SorobanSourceAdapter) Run(ctx context.Context) error {
	log.Printf("Running SorobanSourceAdapter...")

	if method, ok := s.config.Extra["rpc_method"].(string); ok {
		switch method {
		case "getLatestLedger":
			seq, err := s.GetLatestLedger(ctx)
			if err != nil {
				return fmt.Errorf("failed to get latest ledger: %w", err)
			}
			log.Printf("Latest ledger from RPC: %d", seq)
		case "getFeeStats":
			stats, err := s.GetFeeStats(ctx)
			if err != nil {
				return fmt.Errorf("failed to get fee stats: %w", err)
			}
			log.Printf("Fee stats: %+v", stats)
		case "getEvents":
			// Extract filters from the Extra map.
			var filters []EventFilter
			if rawFilters, ok := s.config.Extra["filters"].([]interface{}); ok {
				for _, rf := range rawFilters {
					if fMap, ok := rf.(map[string]interface{}); ok {
						filter := EventFilter{}
						if t, ok := fMap["type"].(string); ok {
							filter.Type = t
						}
						if cids, ok := fMap["contractIds"].([]interface{}); ok {
							for _, cid := range cids {
								if cs, ok := cid.(string); ok {
									filter.ContractIds = append(filter.ContractIds, cs)
								}
							}
						}
						if rawTopics, ok := fMap["topics"].([]interface{}); ok {
							// Expect topics to be an array of arrays.
							for _, rt := range rawTopics {
								if topicArr, ok := rt.([]interface{}); ok {
									var topics []string
									for _, ts := range topicArr {
										if tsStr, ok := ts.(string); ok {
											topics = append(topics, tsStr)
										}
									}
									filter.Topics = append(filter.Topics, topics)
								}
							}
						}
						filters = append(filters, filter)
					}
				}
			}

			// Extract pagination parameters.
			var pagination PaginationOptions
			if rawPagination, ok := s.config.Extra["pagination"].(map[string]interface{}); ok {
				if limit, ok := rawPagination["limit"].(int); ok {
					pagination.Limit = limit
				} else if limitFloat, ok := rawPagination["limit"].(float64); ok {
					pagination.Limit = int(limitFloat)
				}
				if cursor, ok := rawPagination["cursor"].(string); ok {
					pagination.Cursor = cursor
				}
			}

			// Get xdrFormat.
			xdrFormat := "json"
			if xf, ok := s.config.Extra["xdrFormat"].(string); ok {
				xdrFormat = xf
			}

			// Retrieve endLedger if provided in the extra config.
			var endLedger uint64 = 0
			if rawEnd, ok := s.config.Extra["end_ledger"]; ok {
				switch v := rawEnd.(type) {
				case int:
					endLedger = uint64(v)
				case float64:
					endLedger = uint64(v)
				}
			}

			// Call getEvents.
			eventsResp, err := s.GetEvents(ctx, s.config.StartLedger, endLedger, filters, pagination, xdrFormat)
			if err != nil {
				return fmt.Errorf("failed to get events: %w", err)
			}
			log.Printf("Received getEvents response: %+v", eventsResp)
		case "getLedgers":
			return s.runGetLedgersLoop(ctx)
		default:
			log.Printf("Unrecognized rpc_method: %s", method)
		}
	} else {
		log.Printf("No rpc_method specified in config; running default polling loop")
		// Default behavior...
	}

	return nil
}

// =============================================================
// getLedgers Loop Implementation
// =============================================================

// =============================================================
// Checkpointing Functions
// =============================================================

// getCheckpointPath returns the path to the checkpoint file
func (s *SorobanSourceAdapter) getCheckpointPath() string {
	// Get checkpoint_dir from config, default to /tmp/checkpoints/soroban-source
	dir := "/tmp/checkpoints/soroban-source"
	if checkpointDir, ok := s.config.Extra["checkpoint_dir"].(string); ok {
		dir = checkpointDir
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("WARNING: Failed to create checkpoint directory: %v, using /tmp", err)
		dir = "/tmp"
	}

	return filepath.Join(dir, "ledgers.json")
}

// saveCheckpoint saves the current processing state to disk
func (s *SorobanSourceAdapter) saveCheckpoint(ledger, totalProcessed uint64) error {
	checkpoint := &Checkpoint{
		LastProcessedLedger: ledger,
		Timestamp:           time.Now(),
		TotalProcessed:      totalProcessed,
		PipelineName:        "soroban-pipeline",
	}

	path := s.getCheckpointPath()
	tmpPath := path + ".tmp"

	// Marshal to JSON with indentation
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Write to temp file
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename checkpoint: %w", err)
	}

	log.Printf("Saved checkpoint: ledger %d (total: %d)", ledger, totalProcessed)
	return nil
}

// loadCheckpoint loads the processing state from disk
func (s *SorobanSourceAdapter) loadCheckpoint() (*Checkpoint, error) {
	path := s.getCheckpointPath()

	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, nil // No checkpoint, not an error
	}

	// Read file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint: %w", err)
	}

	// Unmarshal JSON
	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		log.Printf("WARNING: Failed to unmarshal checkpoint (corrupted?): %v", err)
		return nil, nil // Corrupted checkpoint, start fresh
	}

	log.Printf("Loaded checkpoint: ledger %d (saved at %v)", checkpoint.LastProcessedLedger, checkpoint.Timestamp)
	return &checkpoint, nil
}

// decodeLedgerXDR decodes a ledger response map into xdr.LedgerCloseMeta.
//
// This function handles the RPC-specific decoding of raw JSON responses. It is NOT
// subject to the CLAUDE.md guideline about using SDK helper methods, which applies
// to downstream transaction processors, not RPC response decoding.
//
// The process:
// 1. Extract base64-encoded XDR from RPC JSON response
// 2. Base64 decode the string
// 3. Use Stellar SDK's xdr.SafeUnmarshal to deserialize into xdr.LedgerCloseMeta
//
// Downstream processors SHOULD use SDK helper methods (tx.GetTransactionEvents(), etc.)
// when processing the resulting LedgerCloseMeta. See docs/stellar-go-sdk-helper-methods.md
func decodeLedgerXDR(ledgerMap map[string]interface{}) (*xdr.LedgerCloseMeta, error) {
	// Extract metadataXdr field from RPC response
	metadataXdrStr, ok := ledgerMap["metadataXdr"].(string)
	if !ok {
		return nil, fmt.Errorf("metadataXdr field not found or not a string")
	}

	// Base64 decode
	xdrBytes, err := base64.StdEncoding.DecodeString(metadataXdrStr)
	if err != nil {
		return nil, fmt.Errorf("failed to base64 decode metadataXdr: %w", err)
	}

	// XDR unmarshal using Stellar SDK function
	var ledgerCloseMeta xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(xdrBytes, &ledgerCloseMeta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

	return &ledgerCloseMeta, nil
}

// runGetLedgersLoop processes ledgers sequentially from start_ledger to end_ledger.
func (s *SorobanSourceAdapter) runGetLedgersLoop(ctx context.Context) error {
	// Try to load checkpoint
	checkpoint, err := s.loadCheckpoint()
	if err != nil {
		log.Printf("WARNING: Failed to load checkpoint: %v", err)
	}

	// Set starting ledger and total processed count
	currentLedger := s.config.StartLedger
	totalProcessed := uint64(0)

	// Resume from checkpoint if available
	if checkpoint != nil && checkpoint.LastProcessedLedger > 0 {
		log.Printf("Resuming from checkpoint: ledger %d", checkpoint.LastProcessedLedger)
		currentLedger = checkpoint.LastProcessedLedger + 1
		totalProcessed = checkpoint.TotalProcessed
	}

	// Detect streaming mode
	streamingMode := s.isStreamingMode()
	endLedger := s.getEndLedger()
	batchSize := uint64(s.config.BatchSize)

	if batchSize == 0 {
		batchSize = 100 // Default batch size
	}

	// Get checkpoint interval from config
	checkpointInterval := uint64(100) // Default: save every 100 ledgers
	if interval, ok := s.config.Extra["checkpoint_interval"].(int); ok {
		checkpointInterval = uint64(interval)
	} else if intervalFloat, ok := s.config.Extra["checkpoint_interval"].(float64); ok {
		checkpointInterval = uint64(intervalFloat)
	}

	// Validate poll_interval for streaming mode
	if streamingMode && s.config.PollInterval < 1*time.Second {
		log.Printf("WARNING: poll_interval too low (%v) for streaming mode, setting to 5s", s.config.PollInterval)
		s.config.PollInterval = 5 * time.Second
	}

	// Log mode
	if streamingMode {
		log.Printf("Running in STREAMING mode (no end_ledger)")
		log.Printf("Starting from ledger %d (batch size: %d, poll interval: %v)", currentLedger, batchSize, s.config.PollInterval)
	} else {
		log.Printf("Running in BATCH mode (end_ledger: %d)", endLedger)
		log.Printf("Processing ledgers from %d to %d (batch size: %d)", currentLedger, endLedger, batchSize)
	}

	for {
		// In batch mode, check if we've reached the end
		if !streamingMode && currentLedger >= endLedger {
			log.Printf("Reached end_ledger %d, stopping", endLedger)
			break
		}
		// Calculate batch end
		batchEnd := currentLedger + batchSize
		if !streamingMode && batchEnd > endLedger {
			batchEnd = endLedger
		}

		log.Printf("Fetching ledgers %d to %d", currentLedger, batchEnd)

		// Fetch ledgers from RPC
		ledgersResp, err := s.GetLedgers(ctx, currentLedger, batchEnd)
		if err != nil {
			log.Printf("ERROR: Failed to fetch ledgers %d-%d: %v", currentLedger, batchEnd, err)
			return fmt.Errorf("getLedgers RPC call failed: %w", err)
		}

		// Check for valid response
		if ledgersResp == nil || ledgersResp.Result == nil || len(ledgersResp.Result.Ledgers) == 0 {
			if streamingMode {
				// In streaming mode, no ledgers means we're caught up
				// Try to fetch latest ledger to show better info
				latest, err := s.GetLatestLedger(ctx)
				if err == nil && latest > 0 {
					log.Printf("Caught up: current=%d, latest=%d, waiting %v...", currentLedger, latest, s.config.PollInterval)
				} else {
					log.Printf("Caught up to latest ledger (currently at %d), waiting %v...", currentLedger, s.config.PollInterval)
				}

				// Wait with context cancellation support
				select {
				case <-time.After(s.config.PollInterval):
					continue
				case <-ctx.Done():
					log.Printf("Context cancelled during wait, saving checkpoint and exiting...")
					if err := s.saveCheckpoint(currentLedger, totalProcessed); err != nil {
						log.Printf("WARNING: Failed to save checkpoint: %v", err)
					}
					return ctx.Err()
				}
			} else {
				// In batch mode, empty response is unexpected
				log.Printf("WARNING: No ledgers returned for range %d-%d", currentLedger, batchEnd)
				continue
			}
		}

		// Extract and process ledgers
		ledgers := ledgersResp.Result.Ledgers

		// Track actually processed ledgers
		var lastProcessedLedger uint64
		processedCount := uint64(0)

		// Decode and send each ledger to processors
		for _, ledger := range ledgers {
			// Convert to map for XDR extraction
			ledgerMap, ok := ledger.(map[string]interface{})
			if !ok {
				log.Printf("WARNING: Ledger is not a map, skipping: %T", ledger)
				continue
			}

			// Extract ledger sequence for tracking
			var ledgerSeq uint64
			if seqFloat, ok := ledgerMap["sequence"].(float64); ok {
				ledgerSeq = uint64(seqFloat)
			} else if seqInt, ok := ledgerMap["sequence"].(int); ok {
				ledgerSeq = uint64(seqInt)
			} else {
				log.Printf("WARNING: Could not extract ledger sequence from map, skipping")
				continue
			}

			// Decode XDR
			ledgerCloseMeta, err := decodeLedgerXDR(ledgerMap)
			if err != nil {
				log.Printf("ERROR: Failed to decode ledger XDR for sequence %d: %v", ledgerSeq, err)
				continue
			}

			// Send decoded XDR to processors
			msg := cdpProcessor.Message{Payload: *ledgerCloseMeta}

			for _, proc := range s.processors {
				if err := proc.Process(ctx, msg); err != nil {
					log.Printf("Processor %T failed for ledger %d: %v", proc, ledgerSeq, err)
					// Continue processing other ledgers
				}
			}

			// Track successful processing
			lastProcessedLedger = ledgerSeq
			processedCount++
		}

		totalProcessed += processedCount

		log.Printf("Processed %d ledgers from batch %d-%d (total: %d)", processedCount, currentLedger, batchEnd, totalProcessed)

		// Advance to next ledger based on what was actually processed
		if processedCount > 0 {
			// Resume from the ledger after the last one we successfully processed
			currentLedger = lastProcessedLedger + 1
		} else {
			// No ledgers processed - in streaming mode this means caught up, in batch mode skip the range
			if !streamingMode {
				log.Printf("WARNING: No ledgers processed in range %d-%d, advancing past range", currentLedger, batchEnd)
			}
			currentLedger = batchEnd
		}

		// Save checkpoint logic
		shouldSaveCheckpoint := false
		if streamingMode && processedCount > 0 {
			// In streaming mode, save checkpoint after every batch that processed ledgers
			shouldSaveCheckpoint = true
		} else if totalProcessed%checkpointInterval == 0 {
			// In batch mode, save based on interval
			shouldSaveCheckpoint = true
		}

		if shouldSaveCheckpoint {
			// Save the last actually processed ledger, not the next one to process
			checkpointLedger := lastProcessedLedger
			if processedCount == 0 {
				// If nothing processed this iteration, use currentLedger - 1
				// (This can happen in batch mode when reaching checkpoint interval)
				if currentLedger > 0 {
					checkpointLedger = currentLedger - 1
				}
			}
			if err := s.saveCheckpoint(checkpointLedger, totalProcessed); err != nil {
				log.Printf("WARNING: Failed to save checkpoint: %v", err)
			}
		}

		// Check for cancellation
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled, stopping at ledger %d", currentLedger)
			return ctx.Err()
		default:
		}

		// Rate limiting
		if s.config.PollInterval > 0 {
			time.Sleep(s.config.PollInterval)
		}
	}

	log.Printf("Completed processing ledgers %d to %d (total: %d)", s.config.StartLedger, endLedger, totalProcessed)

	// Save final checkpoint with the last actually processed ledger
	finalCheckpointLedger := currentLedger
	if currentLedger > 0 {
		finalCheckpointLedger = currentLedger - 1
	}
	if err := s.saveCheckpoint(finalCheckpointLedger, totalProcessed); err != nil {
		log.Printf("WARNING: Failed to save final checkpoint: %v", err)
	}

	return nil
}

// isStreamingMode returns true if streaming mode is enabled (no end_ledger specified)
func (s *SorobanSourceAdapter) isStreamingMode() bool {
	// Check if end_ledger exists in config
	_, hasEndLedger := s.config.Extra["end_ledger"]
	return !hasEndLedger
}

// getEndLedger returns the end ledger from config, or fetches the latest ledger if not specified.
func (s *SorobanSourceAdapter) getEndLedger() uint64 {
	// If streaming mode, return 0
	if s.isStreamingMode() {
		return 0
	}

	// Check if end_ledger specified in config
	if rawEnd, ok := s.config.Extra["end_ledger"]; ok {
		switch v := rawEnd.(type) {
		case int:
			return uint64(v)
		case float64:
			return uint64(v)
		}
	}

	// If no end_ledger specified, fetch latest
	latest, err := s.GetLatestLedger(context.Background())
	if err != nil {
		log.Printf("Failed to get latest ledger: %v, using start + 10000", err)
		return s.config.StartLedger + 10000
	}

	log.Printf("No end_ledger specified, using latest: %d", latest)
	return latest
}

// Subscribe adds a processor to the adapter's processor chain.
// This signature now matches: Subscribe(processor.Processor) error.
func (s *SorobanSourceAdapter) Subscribe(receiver cdpProcessor.Processor) {
	s.processors = append(s.processors, receiver)
	log.Printf("Subscribed processor %T", receiver)
}
