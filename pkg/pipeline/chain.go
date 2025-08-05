package pipeline

import (
	"log"
	
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// BuildProcessorChain chains processors sequentially and subscribes all consumers to the last processor
func BuildProcessorChain(processors []processor.Processor, consumers []processor.Processor) {
	var lastProcessor processor.Processor

	// Chain all processors sequentially
	for _, p := range processors {
		if lastProcessor != nil {
			lastProcessor.Subscribe(p)
			log.Printf("Chained processor %T -> %T", lastProcessor, p)
		}
		lastProcessor = p
	}

	// If any consumers are provided, subscribe them to the last processor
	if lastProcessor != nil {
		for _, c := range consumers {
			lastProcessor.Subscribe(c)
			log.Printf("Chained processor %T -> consumer %T", lastProcessor, c)
		}
	} else if len(consumers) > 0 {
		// If no processors but multiple consumers, chain the consumers
		for i := 1; i < len(consumers); i++ {
			consumers[0].Subscribe(consumers[i])
			log.Printf("Chained consumer %T -> consumer %T", consumers[0], consumers[i])
		}
	}
}