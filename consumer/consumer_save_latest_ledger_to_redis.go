package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveLatestLedgerRedis struct {
	client     *redis.Client
	processors []processor.Processor
	keyPrefix  string
}

func NewSaveLatestLedgerRedis(config map[string]interface{}) (*SaveLatestLedgerRedis, error) {
	address, ok := config["redis_address"].(string)
	if !ok {
		return nil, fmt.Errorf("missing redis_address in config")
	}

	password, _ := config["redis_password"].(string)
	dbNum, _ := config["redis_db"].(int)
	keyPrefix, _ := config["key_prefix"].(string)
	if keyPrefix == "" {
		keyPrefix = "stellar:ledger:"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       dbNum,
	})

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	return &SaveLatestLedgerRedis{
		client:    client,
		keyPrefix: keyPrefix,
	}, nil
}

func (s *SaveLatestLedgerRedis) Subscribe(processor processor.Processor) {
	s.processors = append(s.processors, processor)
}

func (s *SaveLatestLedgerRedis) Process(ctx context.Context, msg processor.Message) error {
	var ledger struct {
		Sequence uint32 `json:"sequence"`
		Hash     string `json:"hash"`
	}

	if err := json.Unmarshal(msg.Payload.([]byte), &ledger); err != nil {
		return fmt.Errorf("error unmarshaling ledger data: %w", err)
	}

	// Store latest ledger info
	pipe := s.client.Pipeline()

	// Store the sequence and hash
	key := s.keyPrefix + "latest"
	data := map[string]interface{}{
		"sequence":   ledger.Sequence,
		"hash":       ledger.Hash,
		"updated_at": time.Now().UTC().Format(time.RFC3339),
	}

	pipe.HSet(ctx, key, data)

	// Also store in a sorted set for history
	historyKey := s.keyPrefix + "history"
	pipe.ZAdd(ctx, historyKey, redis.Z{
		Score:  float64(ledger.Sequence),
		Member: ledger.Hash,
	})

	// Keep only last 1000 ledgers in history
	pipe.ZRemRangeByRank(ctx, historyKey, 0, -1001)

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("error executing Redis pipeline: %w", err)
	}

	log.Printf("Stored latest ledger sequence %d with hash %s", ledger.Sequence, ledger.Hash)
	return nil
}

func (s *SaveLatestLedgerRedis) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
