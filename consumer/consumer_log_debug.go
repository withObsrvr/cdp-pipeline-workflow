package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"github.com/stellar/go/xdr"
)

// LogDebug is a simple consumer that logs messages for debugging
type LogDebug struct {
	processors []processor.Processor
	logLevel   string
}

// NewLogDebug creates a new LogDebug consumer
func NewLogDebug(config map[string]interface{}) (*LogDebug, error) {
	logLevel := "info"
	if level, ok := config["log_level"].(string); ok {
		logLevel = level
	}

	return &LogDebug{
		processors: make([]processor.Processor, 0),
		logLevel:   logLevel,
	}, nil
}

// Subscribe adds a processor to the chain
func (l *LogDebug) Subscribe(processor processor.Processor) {
	l.processors = append(l.processors, processor)
}

// Process logs the incoming message
func (l *LogDebug) Process(ctx context.Context, msg processor.Message) error {
	log.Printf("=== LogDebug Consumer ===")
	
	// Log payload type
	switch payload := msg.Payload.(type) {
	case *xdr.LedgerCloseMeta:
		log.Printf("Payload type: LedgerCloseMeta")
		log.Printf("Ledger sequence: %d", payload.LedgerSequence())
		log.Printf("Ledger close time: %d", payload.LedgerCloseTime())
	case xdr.LedgerCloseMeta:
		log.Printf("Payload type: LedgerCloseMeta (non-pointer)")
		log.Printf("Ledger sequence: %d", payload.LedgerSequence())
		log.Printf("Ledger close time: %d", payload.LedgerCloseTime())
	default:
		log.Printf("Payload type: %T", payload)
	}

	// Log metadata
	if msg.Metadata != nil {
		log.Printf("Metadata keys: %d", len(msg.Metadata))
		for key, value := range msg.Metadata {
			switch v := value.(type) {
			case []processor.TransactionXDROutput:
				log.Printf("  %s: %d TransactionXDROutput items", key, len(v))
			case []processor.WalletBackendOutput:
				log.Printf("  %s: %d WalletBackendOutput items", key, len(v))
				// Log some details of state changes
				totalChanges := 0
				for _, output := range v {
					totalChanges += len(output.Changes)
				}
				log.Printf("    Total state changes: %d", totalChanges)
			case []processor.ParticipantOutput:
				log.Printf("  %s: %d ParticipantOutput items", key, len(v))
				// Log total participants
				totalParticipants := 0
				for _, output := range v {
					totalParticipants += len(output.Participants)
				}
				log.Printf("    Total unique participants: %d", totalParticipants)
			case []processor.StellarEffectsMessage:
				log.Printf("  %s: %d StellarEffectsMessage items", key, len(v))
			case []processor.AccountRecord:
				log.Printf("  %s: %d AccountRecord items", key, len(v))
			case bool:
				log.Printf("  %s: %v", key, v)
			default:
				log.Printf("  %s: %T", key, v)
			}
		}
	} else {
		log.Printf("No metadata attached")
	}

	// Optionally log as JSON if debug level
	if l.logLevel == "debug" {
		if jsonBytes, err := json.MarshalIndent(msg.Metadata, "", "  "); err == nil {
			log.Printf("Metadata JSON:\n%s", string(jsonBytes))
		}
	}

	log.Printf("=== End LogDebug ===")

	// Forward to any subscribers
	log.Printf("LogDebug: Forwarding to %d subscribers", len(l.processors))
	for i, p := range l.processors {
		log.Printf("LogDebug: Forwarding to subscriber %d (%T)", i, p)
		if err := p.Process(ctx, msg); err != nil {
			return fmt.Errorf("error forwarding to processor: %w", err)
		}
	}

	log.Printf("LogDebug: Finished processing")
	return nil
}

// Close does nothing for LogDebug
func (l *LogDebug) Close() error {
	return nil
}