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

// Configuration types
type SorobanConfig struct {
	RPCURL       string
	APIKey       string
	ContractID   string
	StartLedger  uint64
	BatchSize    int
	PollInterval time.Duration
}

// Error types
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	return e.Message
}

// Event statistics
type EventStats struct {
	ProcessedEvents  uint64
	SuccessfulEvents uint64
	FailedEvents     uint64
	LastLedger       uint64
	LastEventTime    time.Time
}

type PaginationOptions struct {
	Limit int `json:"limit"`
}

type GetEventsResponse struct {
	Result struct {
		Events       []Event `json:"events"`
		LatestLedger uint64  `json:"latestLedger"`
	} `json:"result"`
}

type GetLatestLedgerResponse struct {
	Result struct {
		Sequence uint64 `json:"sequence"`
	} `json:"result"`
}

type SorobanSourceAdapter struct {
	config     SorobanConfig
	processors []cdpProcessor.Processor
	client     *http.Client
	lastLedger uint64
}

// RPC request/response structures
type GetEventsRequest struct {
	JSONRPC string                 `json:"jsonrpc"`
	ID      int                    `json:"id"`
	Method  string                 `json:"method"`
	Params  GetEventsRequestParams `json:"params"`
}

type GetEventsRequestParams struct {
	StartLedger uint64            `json:"startLedger"`
	Filters     []EventFilter     `json:"filters"`
	Pagination  PaginationOptions `json:"pagination"`
}

type EventFilter struct {
	Type        string   `json:"type"`
	ContractIds []string `json:"contractIds"`
}

func NewSorobanSourceAdapter(config map[string]interface{}) (SourceAdapter, error) {
	log.Printf("Initializing SorobanSourceAdapter with config: %+v", config)

	rpcURL, ok := config["rpc_url"].(string)
	if !ok {
		return nil, errors.New("rpc_url must be specified")
	}

	apiKey, ok := config["api_key"].(string)
	if !ok {
		return nil, errors.New("api_key must be specified")
	}

	contractID, ok := config["contract_id"].(string)
	if !ok {
		return nil, errors.New("contract_id must be specified")
	}

	// Get other configs with defaults
	batchSize := 100
	if size, ok := config["batch_size"].(int); ok {
		batchSize = size
	}

	adapter := &SorobanSourceAdapter{
		config: SorobanConfig{
			RPCURL:     rpcURL,
			APIKey:     apiKey,
			ContractID: contractID,
			BatchSize:  batchSize,
		},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	// Initialize with latest ledger
	latestLedger, err := adapter.getLatestLedger()
	if err != nil {
		return nil, fmt.Errorf("failed to get latest ledger: %w", err)
	}
	adapter.lastLedger = latestLedger

	log.Printf("Initialized SorobanSourceAdapter starting from ledger: %d", adapter.lastLedger)
	return adapter, nil
}

func (s *SorobanSourceAdapter) getLatestLedger() (uint64, error) {
	req := struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Method  string `json:"method"`
	}{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "getLatestLedger",
	}

	var resp GetLatestLedgerResponse
	if err := s.sendRPCRequest(context.Background(), req, &resp); err != nil {
		return 0, fmt.Errorf("failed to get latest ledger: %w", err)
	}

	log.Printf("Got latest ledger sequence: %d", resp.Result.Sequence)
	return resp.Result.Sequence, nil
}

func (s *SorobanSourceAdapter) Subscribe(receiver cdpProcessor.Processor) {
	log.Printf("Registering processor: %T", receiver)
	s.processors = append(s.processors, receiver)
}

func (s *SorobanSourceAdapter) Run(ctx context.Context) error {
	log.Printf("Starting Soroban event ingestion for contract: %s from ledger: %d",
		s.config.ContractID, s.lastLedger)

	ticker := time.NewTicker(10 * time.Second) // Poll every 10 seconds
	defer ticker.Stop()

	var consecutiveErrors int

	for {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled, shutting down Soroban event ingestion")
			return ctx.Err()
		case <-ticker.C:
			log.Printf("Fetching events starting from ledger %d", s.lastLedger)

			if err := s.fetchAndProcessEvents(ctx); err != nil {
				consecutiveErrors++
				log.Printf("Error fetching events (attempt %d): %v", consecutiveErrors, err)

				if consecutiveErrors > 5 {
					// Increase backoff time after multiple failures
					ticker.Reset(30 * time.Second)
				}
			} else {
				consecutiveErrors = 0
				ticker.Reset(10 * time.Second) // Reset to normal polling interval
			}
		}
	}
}

func (s *SorobanSourceAdapter) fetchAndProcessEvents(ctx context.Context) error {
	req := GetEventsRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "getEvents",
		Params: GetEventsRequestParams{
			StartLedger: s.lastLedger,
			Filters: []EventFilter{
				{
					Type:        "contract",
					ContractIds: []string{s.config.ContractID},
				},
			},
			Pagination: PaginationOptions{
				Limit: s.config.BatchSize,
			},
		},
	}

	var resp GetEventsResponse
	if err := s.sendRPCRequest(ctx, req, &resp); err != nil {
		return errors.Wrap(err, "failed to fetch events")
	}

	log.Printf("Received response from Soroban RPC: Latest Ledger: %d, Events Count: %d",
		resp.Result.LatestLedger, len(resp.Result.Events))

	for i, event := range resp.Result.Events {
		log.Printf("Processing event %d/%d - ID: %s, Type: %s, Ledger: %d",
			i+1, len(resp.Result.Events), event.ID, event.Type, event.Ledger)

		if err := s.processEvent(ctx, event); err != nil {
			log.Printf("Error processing event %s: %v", event.ID, err)
			continue
		}
	}

	if len(resp.Result.Events) > 0 {
		lastEvent := resp.Result.Events[len(resp.Result.Events)-1]
		s.lastLedger = lastEvent.Ledger
		log.Printf("Updated last processed ledger to %d", s.lastLedger)
	} else {
		// If no events, update to latest ledger to avoid re-scanning empty ledgers
		s.lastLedger = resp.Result.LatestLedger
		log.Printf("No new events, updating last ledger to latest: %d", s.lastLedger)
	}

	return nil
}

func (s *SorobanSourceAdapter) processEvent(ctx context.Context, event Event) error {
	log.Printf("Event details - Contract: %s, Ledger: %d, TxHash: %s",
		event.ContractID, event.Ledger, event.TxHash)

	// Log topic and value in a readable format
	topicStr, _ := json.MarshalIndent(event.Topic, "", "  ")
	valueStr, _ := json.MarshalIndent(event.Value, "", "  ")
	log.Printf("Event topic: %s", string(topicStr))
	log.Printf("Event value: %s", string(valueStr))

	for i, processor := range s.processors {
		log.Printf("Sending event to processor %d (%T)", i+1, processor)
		if err := processor.Process(ctx, cdpProcessor.Message{Payload: event}); err != nil {
			return errors.Wrapf(err, "error in processor %T", processor)
		}
	}

	return nil
}

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
	httpReq.Header.Set("Authorization", fmt.Sprintf("Api-Key %s", s.config.APIKey))

	// Log request details
	reqDump, _ := httputil.DumpRequestOut(httpReq, true)
	log.Printf("Sending RPC request:\n%s", string(reqDump))

	httpResp, err := s.client.Do(httpReq)
	if err != nil {
		return errors.Wrap(err, "failed to send request")
	}
	defer httpResp.Body.Close()

	// Log response headers and status
	log.Printf("Received response: Status: %s, Length: %d",
		httpResp.Status, httpResp.ContentLength)

	if httpResp.StatusCode != http.StatusOK {
		body, _ := httputil.DumpResponse(httpResp, true)
		return fmt.Errorf("unexpected status code %d: %s", httpResp.StatusCode, string(body))
	}

	if err := json.NewDecoder(httpResp.Body).Decode(resp); err != nil {
		return errors.Wrap(err, "failed to decode response")
	}

	return nil
}
