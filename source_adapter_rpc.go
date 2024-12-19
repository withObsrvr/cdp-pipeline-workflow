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
	AuthHeader   string
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
		Events       []cdpProcessor.Event `json:"events"`
		LatestLedger uint64               `json:"latestLedger"`
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

	authHeader, ok := config["auth_header"].(string)
	if !ok {
		return nil, errors.New("auth_header must be specified")
	}

	batchSize := 100
	if size, ok := config["batch_size"].(int); ok {
		batchSize = size
	}

	adapter := &SorobanSourceAdapter{
		config: SorobanConfig{
			RPCURL:     rpcURL,
			AuthHeader: authHeader,
			BatchSize:  batchSize,
		},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

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
			Pagination: PaginationOptions{
				Limit: s.config.BatchSize,
			},
		},
	}

	var resp GetEventsResponse
	if err := s.sendRPCRequest(ctx, req, &resp); err != nil {
		return errors.Wrap(err, "failed to fetch events")
	}

	log.Printf("Received %d events from Soroban RPC", len(resp.Result.Events))

	for _, event := range resp.Result.Events {
		if err := s.processEvent(ctx, event); err != nil {
			log.Printf("Error processing event %s: %v", event.ID, err)
			continue
		}
	}

	if len(resp.Result.Events) > 0 {
		lastEvent := resp.Result.Events[len(resp.Result.Events)-1]
		s.lastLedger = lastEvent.Ledger
	} else {
		s.lastLedger = resp.Result.LatestLedger
	}

	return nil
}

func (s *SorobanSourceAdapter) processEvent(ctx context.Context, event cdpProcessor.Event) error {
	log.Printf("Processing event - Ledger: %d, TxHash: %s", event.Ledger, event.TxHash)

	for _, processor := range s.processors {
		if err := processor.Process(ctx, cdpProcessor.Message{Payload: event}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
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
	httpReq.Header.Set("Authorization", s.config.AuthHeader)

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
