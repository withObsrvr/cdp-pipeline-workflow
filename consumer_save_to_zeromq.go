package main

import (
	"context"
	"fmt"
	"log"

	"github.com/zeromq/goczmq"
)

type SaveToZeroMQ struct {
	publisher  *goczmq.Sock
	address    string
	processors []Processor
}

func NewSaveToZeroMQ(config map[string]interface{}) (*SaveToZeroMQ, error) {
	address, ok := config["address"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for SaveToZeroMQ: missing 'address'")
	}

	publisher, err := goczmq.NewPub(address)
	if err != nil {
		return nil, fmt.Errorf("error creating ZeroMQ publisher: %w", err)
	}

	return &SaveToZeroMQ{
		publisher: publisher,
		address:   address,
	}, nil
}

func (z *SaveToZeroMQ) Subscribe(processor Processor) {
	z.processors = append(z.processors, processor)
}

func (z *SaveToZeroMQ) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in SaveToZeroMQ")

	payload, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte type, got %T", msg.Payload)
	}

	_, err := z.publisher.Write(payload)
	if err != nil {
		return fmt.Errorf("error writing to ZeroMQ: %w", err)
	}

	// log.Printf("Payment published to ZeroMQ: %d", msg.Payload)
	return nil
}

func (z *SaveToZeroMQ) Close() error {
	z.publisher.Destroy()
	return nil
}
