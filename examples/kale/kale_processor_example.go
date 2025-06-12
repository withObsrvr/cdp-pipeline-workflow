package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/processor/contract/kale"
)

// SimpleConsumer is a basic consumer that prints received messages
type SimpleConsumer struct {
	name string
}

func (c *SimpleConsumer) Process(ctx context.Context, msg types.Message) error {
	// Parse the message
	jsonData, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte payload, got %T", msg.Payload)
	}

	var metrics kale.KaleBlockMetrics
	if err := json.Unmarshal(jsonData, &metrics); err != nil {
		return fmt.Errorf("error unmarshaling metrics: %w", err)
	}

	// Print the metrics
	log.Printf("Consumer %s received metrics for block %d:", c.name, metrics.BlockIndex)
	log.Printf("  Timestamp: %s", metrics.Timestamp)
	log.Printf("  Participants: %d", metrics.Participants)
	log.Printf("  Total Staked: %d", metrics.TotalStaked)
	log.Printf("  Total Reward: %d", metrics.TotalReward)
	log.Printf("  Highest Zero Count: %d", metrics.HighestZeroCount)
	log.Printf("  Duration: %d ms", metrics.Duration)

	return nil
}

func (c *SimpleConsumer) Subscribe(p types.Processor) {
	// This consumer doesn't need to subscribe to anything
}

func (c *SimpleConsumer) Name() string {
	return c.name
}

func main() {
	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received shutdown signal, stopping...")
		cancel()
	}()

	// Create the Kale processor
	kaleProcessor, err := kale.NewKaleProcessor(map[string]interface{}{
		"contract_id": "CAKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", // Replace with your contract ID
	})
	if err != nil {
		log.Fatalf("Failed to create Kale processor: %v", err)
	}

	// Create and subscribe a consumer
	consumer := &SimpleConsumer{name: "KaleMetricsConsumer"}
	kaleProcessor.Subscribe(consumer)

	log.Println("Kale processor initialized and ready to process events")

	// Example of processing a contract invocation
	invocationExample := map[string]interface{}{
		"contract_id":      "CAKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", // Replace with your contract ID
		"invoking_account": "GAKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", // Replace with a valid account
		"function_name":    "plant",
		"ledger_sequence":  float64(12345),
		"timestamp":        time.Now().Format(time.RFC3339),
		"arguments": []interface{}{
			map[string]interface{}{
				"u32": float64(1), // Block index
			},
		},
	}

	// Convert to JSON
	invocationJSON, err := json.Marshal(invocationExample)
	if err != nil {
		log.Fatalf("Failed to marshal invocation example: %v", err)
	}

	// Process the invocation
	log.Println("Processing example invocation...")
	err = kaleProcessor.Process(ctx, types.Message{
		Payload: invocationJSON,
	})
	if err != nil {
		log.Fatalf("Failed to process invocation: %v", err)
	}

	// Example of processing a contract event
	eventExample := map[string]interface{}{
		"contract_id":     "CAKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", // Replace with your contract ID
		"ledger_sequence": float64(12345),
		"timestamp":       time.Now().Format(time.RFC3339),
		"topic": []interface{}{
			map[string]interface{}{
				"Sym": "work",
			},
			"topic2",
			float64(1), // Block index
			"GAKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", // Farmer address
		},
		"data": map[string]interface{}{
			"zeros": float64(5), // Number of leading zeros
		},
	}

	// Convert to JSON
	eventJSON, err := json.Marshal(eventExample)
	if err != nil {
		log.Fatalf("Failed to marshal event example: %v", err)
	}

	// Process the event
	log.Println("Processing example event...")
	err = kaleProcessor.Process(ctx, types.Message{
		Payload: eventJSON,
	})
	if err != nil {
		log.Fatalf("Failed to process event: %v", err)
	}

	// Print processor stats
	stats := kaleProcessor.GetStats()
	log.Printf("Processor stats:")
	log.Printf("  Processed Events: %d", stats.ProcessedEvents)
	log.Printf("  Plant Events: %d", stats.PlantEvents)
	log.Printf("  Work Events: %d", stats.WorkEvents)
	log.Printf("  Harvest Events: %d", stats.HarvestEvents)
	log.Printf("  Mint Events: %d", stats.MintEvents)
	log.Printf("  Last Processed Time: %s", stats.LastProcessedTime)

	log.Println("Example completed successfully")
}
