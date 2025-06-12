package main

import (
	"context"
	"log"

	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/processor/contract/kale"
)

// KaleProcessorPlugin is a plugin that wraps the KaleProcessor
type KaleProcessorPlugin struct {
	processor *kale.KaleProcessor
}

func (p *KaleProcessorPlugin) Initialize(config map[string]interface{}) error {
	var err error
	p.processor, err = kale.NewKaleProcessor(config)
	return err
}

func (p *KaleProcessorPlugin) Process(ctx context.Context, msg types.Message) error {
	return p.processor.Process(ctx, msg)
}

func (p *KaleProcessorPlugin) Subscribe(processor types.Processor) {
	p.processor.Subscribe(processor)
}

func (p *KaleProcessorPlugin) Name() string {
	return "flow/processor/kale"
}

func (p *KaleProcessorPlugin) Version() string {
	return "1.0.0"
}

func (p *KaleProcessorPlugin) Type() string {
	return "processor"
}

// New creates a new KaleProcessorPlugin
func New() interface{} {
	log.Printf("Creating new KaleProcessorPlugin instance")
	return &KaleProcessorPlugin{}
}

// This is required to make this a valid Go plugin
func main() {}
