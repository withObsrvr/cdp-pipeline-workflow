package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// EventRPC represents an individual event received from the RPC call.
type EventRPC struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	CreatedAt time.Time       `json:"created_at,omitempty"`
	// Additional fields can be added here as needed.
}

// GetEventsResponse represents the response from a getEvents RPC call.
type GetEventsResponse struct {
	Events []EventRPC `json:"events"`
	// You can include pagination or other metadata fields if needed.
}

// GetEventsRPCProcessor processes a GetEventsResponse by iterating over each event,
// marshaling each to JSON, and then sending the resulting message downstream.
type GetEventsRPCProcessor struct {
	processors []Processor
}

// NewGetEventsRPCProcessor creates a new GetEventsRPCProcessor instance.
// Currently, no special configuration is required.
func NewGetEventsRPCProcessor(config map[string]interface{}) (*GetEventsRPCProcessor, error) {
	return &GetEventsRPCProcessor{
		processors: make([]Processor, 0),
	}, nil
}

// Subscribe adds a downstream processor.
func (p *GetEventsRPCProcessor) Subscribe(proc Processor) {
	p.processors = append(p.processors, proc)
	log.Printf("GetEventsRPCProcessor: subscribed downstream processor: %T", proc)
}

// Process expects a Message whose Payload is of type *GetEventsResponse.
// It iterates through each event, marshals it to JSON, and sends it downstream.
func (p *GetEventsRPCProcessor) Process(ctx context.Context, msg Message) error {
	// Expect the payload to be a *GetEventsResponse
	response, ok := msg.Payload.(*GetEventsResponse)
	if !ok {
		return fmt.Errorf("GetEventsRPCProcessor expected payload of type *GetEventsResponse, got %T", msg.Payload)
	}

	// If there are no events, do nothing.
	if response == nil || len(response.Events) == 0 {
		log.Printf("GetEventsRPCProcessor: no events to process")
		return nil
	}

	// Process each event individually.
	for _, event := range response.Events {
		log.Printf("GetEventsRPCProcessor: processing event ID %s, type %s", event.ID, event.Type)

		// Marshal the event structure to JSON.
		data, err := json.Marshal(event)
		if err != nil {
			log.Printf("GetEventsRPCProcessor: failed to marshal event: %v", err)
			continue
		}

		newMsg := Message{
			Payload: data,
		}

		// Forward the message to all subscribed downstream processors.
		for _, proc := range p.processors {
			if err := proc.Process(ctx, newMsg); err != nil {
				log.Printf("GetEventsRPCProcessor: downstream processor error: %v", err)
			}
		}
	}

	return nil
}
