package stellarrpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/source/base"
)

// Protocol structures
type LedgerInfo struct {
	Sequence           uint32          `json:"sequence"`
	Hash               string          `json:"hash"`
	LedgerCloseTime    int64           `json:"ledger_close_time"`
	LedgerMetadata     string          `json:"ledger_metadata,omitempty"`
	LedgerMetadataJSON json.RawMessage `json:"ledger_metadata_json,omitempty"`
}

type GetLedgersRequest struct {
	StartLedger uint32                   `json:"start_ledger,omitempty"`
	Pagination  *LedgerPaginationOptions `json:"pagination,omitempty"`
	Format      string                   `json:"format,omitempty"`
}

type LedgerPaginationOptions struct {
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

type GetLedgersResponse struct {
	Ledgers []LedgerInfo `json:"ledgers"`
	Next    *string      `json:"next,omitempty"`
}

type GetLatestLedgerResponse struct {
	Sequence uint32 `json:"sequence"`
}

// StellarRPCSourceAdapter ingests ledger data from a Stellar RPC endpoint
type StellarRPCSourceAdapter struct {
	endpoint      string
	apiKey        string
	pollInterval  time.Duration
	currentLedger uint32
	format        string // Format to use when retrieving ledgers: "base64" or "json"
	stopCh        chan struct{}
	wg            sync.WaitGroup
	processors    []types.Processor
	ledgerMu      sync.Mutex
	client        *http.Client
}

// NewStellarRPCSourceAdapter creates a new StellarRPCSourceAdapter
func NewStellarRPCSourceAdapter(config map[string]interface{}) (base.SourceAdapter, error) {
	adapter := &StellarRPCSourceAdapter{
		pollInterval: 2 * time.Second,
		format:       "base64", // Default format
		stopCh:       make(chan struct{}),
		processors:   make([]types.Processor, 0),
	}

	// Extract endpoint (required)
	endpoint, ok := config["rpc_endpoint"].(string)
	if !ok {
		return nil, fmt.Errorf("rpc_endpoint configuration is required")
	}
	adapter.endpoint = endpoint

	// Extract API key (optional)
	apiKey, _ := config["api_key"].(string)
	adapter.apiKey = apiKey

	// Extract poll interval (optional, default: 2s)
	if interval, ok := config["poll_interval"].(float64); ok {
		adapter.pollInterval = time.Duration(interval) * time.Second
	}

	// Extract starting ledger (optional)
	if startLedger, ok := config["start_ledger"].(float64); ok {
		adapter.currentLedger = uint32(startLedger)
	}

	// Extract format (optional, default: "base64")
	adapter.format = "base64"
	if format, ok := config["format"].(string); ok {
		if format == "json" || format == "base64" {
			adapter.format = format
		} else {
			log.Printf("Warning: Invalid format '%s', using default 'base64'", format)
		}
	}
	log.Printf("Using format: %s", adapter.format)

	// Create HTTP client with API key if provided
	adapter.client = &http.Client{}
	if apiKey != "" {
		adapter.client.Transport = &transportWithAPIKey{
			apiKey: apiKey,
			rt:     http.DefaultTransport,
		}
	}

	log.Printf("StellarRPCSourceAdapter initialized with endpoint: %s, poll interval: %s", endpoint, adapter.pollInterval)
	return adapter, nil
}

// Subscribe adds a processor to this source adapter
func (s *StellarRPCSourceAdapter) Subscribe(processor types.Processor) {
	s.ledgerMu.Lock()
	defer s.ledgerMu.Unlock()
	s.processors = append(s.processors, processor)
	log.Printf("Processor %T subscribed to StellarRPCSourceAdapter", processor)
}

// Run starts the adapter and begins polling for ledgers
func (s *StellarRPCSourceAdapter) Run(ctx context.Context) error {
	log.Printf("Starting StellarRPCSourceAdapter with endpoint: %s", s.endpoint)

	// If no current ledger is set, get the latest ledger
	if s.currentLedger == 0 {
		latest, err := s.getLatestLedger(ctx)
		if err != nil {
			log.Printf("Failed to get latest ledger: %v, using a default starting point", err)
			s.currentLedger = 1 // Use a reasonable default if we can't get the latest
		} else {
			// Start from a few ledgers back from the latest to avoid any issues
			// with very recent ledgers that might not be fully available
			if latest > 100 {
				s.currentLedger = latest - 100
			} else {
				s.currentLedger = 1
			}
			log.Printf("Starting from ledger: %d (latest was: %d)", s.currentLedger, latest)
		}
	} else {
		log.Printf("Starting from specified ledger: %d", s.currentLedger)
	}

	s.wg.Add(1)
	go s.pollLedgers(ctx)

	// Block until context is done
	<-ctx.Done()
	log.Printf("Context done, stopping Stellar RPC adapter")

	// Signal the polling goroutine to stop
	close(s.stopCh)

	// Wait for the polling goroutine to finish
	s.wg.Wait()
	log.Printf("Stellar RPC adapter stopped")

	return ctx.Err()
}

// pollLedgers continuously polls for new ledgers
func (s *StellarRPCSourceAdapter) pollLedgers(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	// Track consecutive errors to adjust behavior
	consecutiveErrors := 0
	maxConsecutiveErrors := 5

	for {
		select {
		case <-s.stopCh:
			log.Printf("Ledger polling stopped")
			return
		case <-ticker.C:
			err := s.fetchAndProcessLedger(ctx)
			if err != nil {
				log.Printf("Error processing ledger: %v", err)
				consecutiveErrors++

				// If we're having persistent issues, try skipping ahead or getting a new latest ledger
				if consecutiveErrors >= maxConsecutiveErrors {
					log.Printf("Encountered %d consecutive errors, attempting recovery", consecutiveErrors)
					if err := s.attemptRecovery(ctx); err != nil {
						log.Printf("Recovery attempt failed: %v", err)
					} else {
						consecutiveErrors = 0
					}
				}
			} else {
				// Reset error counter on success
				consecutiveErrors = 0
			}
		}
	}
}

// attemptRecovery tries to recover from persistent errors by getting a new latest ledger
// or skipping ahead if needed
func (s *StellarRPCSourceAdapter) attemptRecovery(ctx context.Context) error {
	// Try getting the latest ledger
	latest, err := s.getLatestLedger(ctx)
	if err != nil {
		// If we can't get the latest ledger, skip ahead by 100
		log.Printf("Failed to get latest ledger for recovery: %v, skipping ahead by 100", err)
		s.currentLedger += 100
		return nil
	}

	// If our current ledger is more than 1000 behind, jump forward
	if latest > s.currentLedger+1000 {
		s.currentLedger = latest - 100
		log.Printf("Current ledger was too far behind, jumped to ledger %d", s.currentLedger)
	} else {
		// Otherwise, just skip ahead by 10 ledgers
		s.currentLedger += 10
		log.Printf("Skipped ahead by 10 ledgers to %d", s.currentLedger)
	}

	return nil
}

// fetchAndProcessLedger fetches a ledger and sends it to processors
func (s *StellarRPCSourceAdapter) fetchAndProcessLedger(ctx context.Context) error {
	log.Printf("Fetching ledger %d from RPC endpoint", s.currentLedger)

	// Create the request
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getLedgers",
		"params": GetLedgersRequest{
			StartLedger: s.currentLedger,
			Pagination: &LedgerPaginationOptions{
				Limit: 1,
			},
			Format: s.format,
		},
	}

	// Serialize to JSON
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request with the JSON body
	req, err := http.NewRequestWithContext(ctx, "POST", s.endpoint, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	log.Printf("Sending request to %s with body: %s", s.endpoint, string(jsonBody))
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check status
	log.Printf("Received response with status: %d", resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		// If not OK, try to read the error response body
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Error response body: %s", string(body))
		// Skip to the next ledger if we get a server error
		log.Printf("Skipping ledger %d due to server error", s.currentLedger)
		s.currentLedger++
		return nil
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	log.Printf("Response body: %s", string(body))

	// Parse response
	var result struct {
		Result GetLedgersResponse `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Check for JSON-RPC error
	if result.Error != nil {
		log.Printf("JSON-RPC error: %d - %s", result.Error.Code, result.Error.Message)
		// Skip to the next ledger if we get a server error
		log.Printf("Skipping ledger %d due to JSON-RPC error", s.currentLedger)
		s.currentLedger++
		return nil
	}

	// Check if we got any ledgers
	if len(result.Result.Ledgers) == 0 {
		log.Printf("No ledgers available starting from %d, waiting for next poll", s.currentLedger)
		return nil
	}

	// Process the ledger
	ledger := result.Result.Ledgers[0]
	if ledger.Sequence != s.currentLedger {
		log.Printf("Warning: Requested ledger %d but got %d", s.currentLedger, ledger.Sequence)
	}

	log.Printf("Processing ledger %d with hash %s", ledger.Sequence, ledger.Hash)

	var payload interface{}
	var format string

	// Process based on the configured format
	if s.format == "base64" {
		// Try to convert the ledger to XDR format
		ledgerXDR, err := s.convertToXDR(ledger)
		if err != nil {
			log.Printf("Warning: Could not convert ledger to XDR format: %v", err)
			return fmt.Errorf("failed to convert ledger to XDR format: %w", err)
		}
		payload = *ledgerXDR
		format = "xdr"
		log.Printf("Using XDR format for ledger %d", ledger.Sequence)
	} else {
		// Using JSON format
		// Check if we have JSON metadata directly
		if ledger.LedgerMetadataJSON != nil && len(ledger.LedgerMetadataJSON) > 0 {
			// Parse the JSON metadata
			var parsedMetadata map[string]interface{}
			if err := json.Unmarshal(ledger.LedgerMetadataJSON, &parsedMetadata); err != nil {
				log.Printf("Error parsing JSON metadata: %v", err)
				// Fall back to raw metadata
				payload = ledger.LedgerMetadataJSON
				format = "raw_json"
			} else {
				// Successfully parsed JSON metadata
				payload = parsedMetadata
				format = "json"
				log.Printf("Using parsed JSON metadata for ledger %d", ledger.Sequence)
			}
		} else if ledger.LedgerMetadata != "" {
			// Try to parse the metadata as JSON
			var parsedMetadata map[string]interface{}
			if err := json.Unmarshal([]byte(ledger.LedgerMetadata), &parsedMetadata); err != nil {
				log.Printf("Error parsing ledger metadata as JSON: %v", err)
				// Fall back to raw metadata
				payload = []byte(ledger.LedgerMetadata)
				format = "raw"
			} else {
				// Successfully parsed JSON
				payload = parsedMetadata
				format = "json"
				log.Printf("Using parsed JSON from metadata for ledger %d", ledger.Sequence)
			}
		} else {
			// No metadata available
			log.Printf("No metadata available for ledger %d", ledger.Sequence)
			payload = map[string]interface{}{} // Empty map
			format = "empty"
		}
	}

	// Create the message to be processed
	msg := types.Message{
		Payload: payload,
	}

	// Add metadata to the payload (since Message doesn't have a Metadata field)
	if metadata, ok := payload.(map[string]interface{}); ok {
		metadata["ledger_sequence"] = ledger.Sequence
		metadata["ledger_hash"] = ledger.Hash
		metadata["source"] = "stellar-rpc"
		metadata["format"] = format
	}

	// Process through each processor
	if err := s.processMessageWithProcessors(ctx, msg); err != nil {
		return fmt.Errorf("error in processor chain: %w", err)
	}

	// Move to the next ledger
	s.currentLedger++
	log.Printf("Successfully processed ledger %d, moving to %d", ledger.Sequence, s.currentLedger)

	return nil
}

// convertToXDR attempts to convert a ledger from RPC format to XDR format
func (s *StellarRPCSourceAdapter) convertToXDR(ledger LedgerInfo) (*xdr.LedgerCloseMeta, error) {
	// Check if we have the XDR data directly
	if ledger.LedgerMetadata != "" {
		var ledgerCloseMeta xdr.LedgerCloseMeta

		// Try to decode the base64-encoded XDR data
		xdrBytes, err := base64.StdEncoding.DecodeString(ledger.LedgerMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to decode XDR data: %w", err)
		}

		// Unmarshal the XDR data
		if err := xdr.SafeUnmarshal(xdrBytes, &ledgerCloseMeta); err != nil {
			return nil, fmt.Errorf("failed to unmarshal XDR data: %w", err)
		}

		return &ledgerCloseMeta, nil
	}

	// If we don't have XDR data directly, we would need to construct it from the JSON data
	return nil, fmt.Errorf("XDR conversion from JSON not implemented")
}

// processMessageWithProcessors processes the message through all registered processors
func (s *StellarRPCSourceAdapter) processMessageWithProcessors(ctx context.Context, msg types.Message) error {
	// Get a copy of the processors to avoid holding the lock during processing
	s.ledgerMu.Lock()
	processors := make([]types.Processor, len(s.processors))
	copy(processors, s.processors)
	s.ledgerMu.Unlock()

	// Check if we have any processors
	if len(processors) == 0 {
		log.Printf("Warning: No processors registered")
		return nil
	}

	// Process through each processor in sequence
	for i, proc := range processors {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			procStart := time.Now()

			if err := proc.Process(ctx, msg); err != nil {
				log.Printf("Error in processor %d (%T): %v", i, proc, err)
				return fmt.Errorf("processor %d (%T) failed: %w", i, proc, err)
			}

			processingTime := time.Since(procStart)
			if processingTime > time.Second {
				log.Printf("Warning: Processor %d (%T) took %v to process", i, proc, processingTime)
			} else {
				log.Printf("Processor %d (%T) processed successfully in %v", i, proc, processingTime)
			}
		}
	}

	return nil
}

// getLatestLedger gets the latest ledger from the RPC endpoint
func (s *StellarRPCSourceAdapter) getLatestLedger(ctx context.Context) (uint32, error) {
	// Create the request
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getLatestLedger",
	}

	// Serialize to JSON
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request with JSON body
	req, err := http.NewRequestWithContext(ctx, "POST", s.endpoint, bytes.NewBuffer(jsonBody))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	log.Printf("Sending getLatestLedger request to %s with body: %s", s.endpoint, string(jsonBody))
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check status
	log.Printf("Received getLatestLedger response with status: %d", resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		// Read and log error body
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Error response body: %s", string(body))
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response body: %w", err)
	}
	log.Printf("getLatestLedger response body: %s", string(body))

	// Parse response
	var result struct {
		Result GetLatestLedgerResponse `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	// Check for JSON-RPC error
	if result.Error != nil {
		log.Printf("JSON-RPC error: %d - %s", result.Error.Code, result.Error.Message)
		return 0, fmt.Errorf("JSON-RPC error: %s", result.Error.Message)
	}

	log.Printf("Latest ledger from RPC: %d", result.Result.Sequence)
	return result.Result.Sequence, nil
}

// transportWithAPIKey adds API key to requests
type transportWithAPIKey struct {
	apiKey string
	rt     http.RoundTripper
}

// RoundTrip implements the http.RoundTripper interface
func (t *transportWithAPIKey) RoundTrip(req *http.Request) (*http.Response, error) {
	// Make sure the API key has the 'Api-Key ' prefix if it doesn't already
	apiKeyValue := t.apiKey
	if !strings.HasPrefix(apiKeyValue, "Api-Key ") {
		apiKeyValue = "Api-Key " + apiKeyValue
	}

	req.Header.Set("Authorization", apiKeyValue)
	return t.rt.RoundTrip(req)
}
