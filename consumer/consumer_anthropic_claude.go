package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// AnthropicClaudeConsumer batches messages and sends them to the Anthropic/Claude API.
type AnthropicClaudeConsumer struct {
	// Configuration parameters.
	apiKey        string
	apiURL        string
	batchSize     int
	flushInterval time.Duration

	// Batch storage, protected by a mutex.
	batch   []processor.Message
	batchMu sync.Mutex

	// HTTP client and downstream processors.
	httpClient *http.Client
	processors []processor.Processor

	// Instrumentation.
	totalMessagesSent int64
}

// NewAnthropicClaudeConsumer creates a new instance using the provided configuration.
func NewAnthropicClaudeConsumer(config map[string]interface{}) (*AnthropicClaudeConsumer, error) {
	apiKey, ok := config["anthropic_api_key"].(string)
	if !ok || apiKey == "" {
		return nil, fmt.Errorf("invalid configuration: missing 'anthropic_api_key'")
	}
	apiURL, ok := config["anthropic_api_url"].(string)
	if !ok || apiURL == "" {
		return nil, fmt.Errorf("invalid configuration: missing 'anthropic_api_url'")
	}

	// Get batch settings from config, or use defaults.
	batchSize := 10
	if bs, ok := config["batch_size"].(float64); ok && int(bs) > 0 {
		batchSize = int(bs)
	}
	flushSec := 30
	if interval, ok := config["flush_interval_seconds"].(float64); ok && interval > 0 {
		flushSec = int(interval)
	}

	consumer := &AnthropicClaudeConsumer{
		apiKey:        apiKey,
		apiURL:        apiURL,
		batchSize:     batchSize,
		flushInterval: time.Duration(flushSec) * time.Second,
		batch:         make([]processor.Message, 0, batchSize),
		httpClient:    &http.Client{Timeout: 15 * time.Second},
	}

	// Start periodic flushing of messages in a separate goroutine,
	// similar to how the SaveToRedis consumer operates.
	go consumer.periodicFlush()

	return consumer, nil
}

// Subscribe registers a downstream processor with this consumer.
func (c *AnthropicClaudeConsumer) Subscribe(proc processor.Processor) {
	c.processors = append(c.processors, proc)
}

// Process appends a new message to the current batch and flushes it if the batch size threshold is reached.
func (c *AnthropicClaudeConsumer) Process(ctx context.Context, msg processor.Message) error {
	c.batchMu.Lock()
	c.batch = append(c.batch, msg)
	batchLength := len(c.batch)
	c.batchMu.Unlock()

	if batchLength >= c.batchSize {
		return c.flushBatch(ctx)
	}
	return nil
}

// periodicFlush runs in a background goroutine to flush any pending messages at regular intervals.
func (c *AnthropicClaudeConsumer) periodicFlush() {
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := c.flushBatch(context.Background()); err != nil {
			log.Printf("Error flushing batch: %v", err)
		}
	}
}

// flushBatch sends the current batch of messages to the Anthropic/Claude API.
// It builds a chat-style request payload using a top-level "system" parameter and a "messages" array for user content.
func (c *AnthropicClaudeConsumer) flushBatch(ctx context.Context) error {
	// Grab the current batch.
	c.batchMu.Lock()
	if len(c.batch) == 0 {
		c.batchMu.Unlock()
		return nil
	}
	batchToSend := c.batch
	c.batch = make([]processor.Message, 0, c.batchSize)
	c.batchMu.Unlock()

	// Top-level system instruction.
	systemMessage := "You are a blockchain expert. Analyze the payment transactions below and provide insights regarding trends, anomalies, or any interesting patterns."

	// Build the user message content from the batch.
	userMessageContent := ""
	for i, m := range batchToSend {
		payloadBytes, err := json.Marshal(m.Payload)
		if err != nil {
			log.Printf("Error marshaling message payload: %v", err)
			continue
		}
		userMessageContent += fmt.Sprintf("=== Transaction %d ===\n%s\n\n", i+1, string(payloadBytes))
	}

	// Construct the request body with top-level "system" and a "messages" array for user content.
	requestBody := map[string]interface{}{
		"system":      systemMessage,
		"messages":    []map[string]string{{"role": "user", "content": userMessageContent}},
		"max_tokens":  700,
		"temperature": 0.7,
		"model":       "claude-3-5-sonnet-20241022", // Adjust the model name if needed.
	}
	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("error marshaling request body: %w", err)
	}

	// Create an HTTP request to send to Anthropic/Claude.
	req, err := http.NewRequestWithContext(ctx, "POST", c.apiURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %w", err)
	}

	// Set the required headers.
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("content-type", "application/json")

	// Instrumentation: measure the API call duration.
	start := time.Now()
	resp, err := c.httpClient.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		return fmt.Errorf("error sending request to Anthropic Claude API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var respBody bytes.Buffer
		_, _ = respBody.ReadFrom(resp.Body)
		return fmt.Errorf("received non-200 status: %d, body: %s", resp.StatusCode, respBody.String())
	}

	// Print out the API response
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %v", err)
	} else {
		log.Printf("Anthropic API response: %s", string(respData))
	}

	// Log processing time and update instrumentation metrics.
	if elapsed > 100*time.Millisecond {
		log.Printf("Flushed batch of %d messages in %s", len(batchToSend), elapsed)
	} else {
		log.Printf("Flushed batch of %d messages", len(batchToSend))
	}
	c.totalMessagesSent += int64(len(batchToSend))
	return nil
}
