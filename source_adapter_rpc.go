package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/pkg/errors"
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
	Result interface{} `json:"result"`
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

	// Optionally log the request details (enable/disable debug logging as needed).
	reqDump, _ := httputil.DumpRequestOut(httpReq, true)
	log.Printf("Sending RPC request:\n%s", string(reqDump))

	httpResp, err := s.client.Do(httpReq)
	if err != nil {
		return errors.Wrap(err, "failed to send request")
	}
	defer httpResp.Body.Close()

	log.Printf("Received response: Status: %s, Length: %d", httpResp.Status, httpResp.ContentLength)
	if httpResp.StatusCode != http.StatusOK {
		body, _ := httputil.DumpResponse(httpResp, true)
		return fmt.Errorf("unexpected status code %d: %s", httpResp.StatusCode, string(body))
	}

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
func (s *SorobanSourceAdapter) GetLedgers(ctx context.Context, from uint64, to uint64) (*LedgersResponse, error) {
	params := map[string]interface{}{
		"from": from,
		"to":   to,
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
		default:
			log.Printf("Unrecognized rpc_method: %s", method)
		}
	} else {
		log.Printf("No rpc_method specified in config; running default polling loop")
		// Default behavior...
	}

	return nil
}

// Subscribe adds a processor to the adapter's processor chain.
// This signature now matches: Subscribe(processor.Processor) error.
func (s *SorobanSourceAdapter) Subscribe(receiver cdpProcessor.Processor) {
	s.processors = append(s.processors, receiver)
	log.Printf("Subscribed processor %T", receiver)
}
