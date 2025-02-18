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

// RPCSource is a new source adapter that fetches raw JSON-RPC data and sends it to subscribed processors.
type RPCSource struct {
	config     SorobanConfig
	processors []cdpProcessor.Processor
	client     *http.Client
}

// NewRPCSource creates a new RPCSource instance using the provided configuration.
func NewRPCSourceAdapter(config map[string]interface{}) (SourceAdapter, error) {
	log.Printf("Initializing RPCSource with config: %+v", config)

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

	// Build extra configuration keys (everything except known keys).
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

	source := &RPCSource{
		config: SorobanConfig{
			RPCURL:       rpcURL,
			AuthHeader:   authHeader,
			BatchSize:    batchSize,
			PollInterval: pollInterval,
			StartLedger:  startLedger,
			Extra:        extra,
		},
		client: &http.Client{Timeout: 30 * time.Second},
	}

	return source, nil
}

// Subscribe adds a processor to the RPCSource.
func (s *RPCSource) Subscribe(proc cdpProcessor.Processor) {
	s.processors = append(s.processors, proc)
	log.Printf("Processor %T subscribed", proc)
}

// Run continuously fetches raw RPC data based on the configured rpc_method and sends it to subscribed processors.
func (s *RPCSource) Run(ctx context.Context) error {
	// Create a ticker for polling at the desired interval.
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()

	log.Printf("Starting RPCSource polling loop with interval: %s", s.config.PollInterval)

	for {
		// Check if context is done.
		select {
		case <-ctx.Done():
			log.Printf("RPCSource polling loop exiting due to context cancellation")
			return ctx.Err()
		default:
		}

		log.Printf("Running RPCSource polling iteration...")
		method, ok := s.config.Extra["rpc_method"].(string)
		if !ok {
			log.Printf("No rpc_method specified in config; skipping poll iteration")
		} else {
			var rawData interface{}
			var err error

			switch method {
			case "getLatestLedger":
				rawData, err = s.getLatestLedger(ctx)
				if err != nil {
					log.Printf("Failed to get latest ledger: %v", err)
				}
			case "getFeeStats":
				rawData, err = s.getFeeStats(ctx)
				if err != nil {
					log.Printf("Failed to get fee stats: %v", err)
				}
			case "getEvents":
				// Prepare request parameters by extracting filters and pagination from the Extra config.
				filters, ferr := s.extractFilters()
				if ferr != nil {
					log.Printf("Failed to extract filters: %v", ferr)
					break
				}
				pagination := s.extractPagination()
				xdrFormat := "json"
				if xf, ok := s.config.Extra["xdrFormat"].(string); ok {
					xdrFormat = xf
				}
				var endLedger uint64 = 0
				if rawEnd, ok := s.config.Extra["end_ledger"]; ok {
					switch v := rawEnd.(type) {
					case int:
						endLedger = uint64(v)
					case float64:
						endLedger = uint64(v)
					}
				}
				rawData, err = s.getEvents(ctx, s.config.StartLedger, endLedger, filters, pagination, xdrFormat)
				if err != nil {
					log.Printf("Failed to get events: %v", err)
				} else {
					// Log a short summary; the actual raw data is not printed.
					log.Printf("Fetched getEvents response successfully - passing data to processors")
				}
			default:
				log.Printf("Unrecognized rpc_method: %s", method)
			}

			// Forward the fetched data downstream if available.
			if rawData != nil {
				msg := cdpProcessor.Message{Payload: rawData}
				log.Printf("Forwarding RPC response to processors")
				for _, proc := range s.processors {
					if err := proc.Process(ctx, msg); err != nil {
						log.Printf("Processor %T failed to process data: %v", proc, err)
					}
				}
			}
		}

		// Wait for the next poll tick or context cancellation.
		select {
		case <-ticker.C:
			// Proceed with next iteration.
		case <-ctx.Done():
			log.Printf("RPCSource polling loop exiting due to context cancellation")
			return ctx.Err()
		}
	}
}

// getLatestLedger performs the getLatestLedger RPC call.
func (s *RPCSource) getLatestLedger(ctx context.Context) (uint64, error) {
	var resp GetLatestLedgerResponse
	if err := s.callRPC(ctx, "getLatestLedger", nil, &resp); err != nil {
		return 0, err
	}
	return resp.Result.Sequence, nil
}

// getFeeStats performs the getFeeStats RPC call.
func (s *RPCSource) getFeeStats(ctx context.Context) (*FeeStatsResponse, error) {
	var resp FeeStatsResponse
	if err := s.callRPC(ctx, "getFeeStats", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// getEvents performs the getEvents RPC call and decodes the response into the shared type.
func (s *RPCSource) getEvents(ctx context.Context, startLedger, endLedger uint64, filters []EventFilter, pagination PaginationOptions, xdrFormat string) (*cdpProcessor.GetEventsResponse, error) {
	params := GetEventsRequestParams{
		StartLedger: startLedger,
		EndLedger:   endLedger,
		Filters:     filters,
		Pagination:  pagination,
		XDRFormat:   xdrFormat,
	}
	var resp cdpProcessor.GetEventsResponse
	if err := s.callRPC(ctx, "getEvents", params, &resp); err != nil {
		return nil, fmt.Errorf("getEvents call failed: %w", err)
	}
	return &resp, nil
}

// callRPC constructs and executes a JSON-RPC call.
func (s *RPCSource) callRPC(ctx context.Context, method string, params interface{}, result interface{}) error {
	reqPayload := RPCRequest{
		JSONRPC: "2.0",
		ID:      1, // In production, use unique IDs.
		Method:  method,
		Params:  params,
	}
	return s.sendRPCRequest(ctx, reqPayload, result)
}

// sendRPCRequest performs the HTTP POST call to the RPC endpoint.
func (s *RPCSource) sendRPCRequest(ctx context.Context, req interface{}, result interface{}) error {
	jsonData, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "failed to marshal request")
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", s.config.RPCURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return errors.Wrap(err, "failed to create HTTP request")
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", s.config.AuthHeader)

	// Log the outgoing request for debugging.
	reqDump, _ := httputil.DumpRequestOut(httpReq, true)
	log.Printf("Sending RPC request:\n%s", string(reqDump))

	httpResp, err := s.client.Do(httpReq)
	if err != nil {
		return errors.Wrap(err, "failed to send request")
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := httputil.DumpResponse(httpResp, true)
		return fmt.Errorf("unexpected status code %d: %s", httpResp.StatusCode, string(body))
	}
	if err := json.NewDecoder(httpResp.Body).Decode(result); err != nil {
		return errors.Wrap(err, "failed to decode response")
	}
	return nil
}

// extractFilters constructs the slice of EventFilter from the Extra config.
func (s *RPCSource) extractFilters() ([]EventFilter, error) {
	var filters []EventFilter
	rawFilters, ok := s.config.Extra["filters"].([]interface{})
	if !ok {
		return filters, nil
	}
	for _, rf := range rawFilters {
		fMap, ok := rf.(map[string]interface{})
		if !ok {
			continue
		}
		filter := EventFilter{}
		if t, ok := fMap["type"].(string); ok {
			filter.Type = t
		}
		if cids, ok := fMap["contractIds"].([]interface{}); ok {
			for _, cid := range cids {
				if str, ok := cid.(string); ok {
					filter.ContractIds = append(filter.ContractIds, str)
				}
			}
		}
		if rawTopics, ok := fMap["topics"].([]interface{}); ok {
			for _, rt := range rawTopics {
				topicArr, ok := rt.([]interface{})
				if !ok {
					continue
				}
				var topics []string
				for _, t := range topicArr {
					if tStr, ok := t.(string); ok {
						topics = append(topics, tStr)
					}
				}
				filter.Topics = append(filter.Topics, topics)
			}
		}
		filters = append(filters, filter)
	}
	return filters, nil
}

// extractPagination extracts PaginationOptions from the Extra config.
func (s *RPCSource) extractPagination() PaginationOptions {
	pag := PaginationOptions{}
	if rawPagination, ok := s.config.Extra["pagination"].(map[string]interface{}); ok {
		if limit, ok := rawPagination["limit"].(int); ok {
			pag.Limit = limit
		} else if limitFloat, ok := rawPagination["limit"].(float64); ok {
			pag.Limit = int(limitFloat)
		}
		if cursor, ok := rawPagination["cursor"].(string); ok {
			pag.Cursor = cursor
		}
	}
	return pag
}
