package consumer

import (
	"context"
	"fmt"
	"log"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"github.com/zeromq/goczmq"
)

type SaveToZeroMQ struct {
	publisher  *goczmq.Sock
	address    string
	processors []processor.Processor
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

func (z *SaveToZeroMQ) Subscribe(processor processor.Processor) {
	z.processors = append(z.processors, processor)
}

func (z *SaveToZeroMQ) Process(ctx context.Context, msg processor.Message) error {
	log.Printf("Processing message in SaveToZeroMQ")

	payload, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte type, got %T", msg.Payload)
	}

	_, err := z.publisher.Write(payload)
	if err != nil {
		return fmt.Errorf("error writing to ZeroMQ: %w", err)
	}

	return nil
}

func (z *SaveToZeroMQ) Close() error {
	z.publisher.Destroy()
	return nil
}
