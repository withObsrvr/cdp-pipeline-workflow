package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"google.golang.org/api/option"
)

// PublishToGooglePubSub publishes EventPayment messages to a Google Pub/Sub topic
type PublishToGooglePubSub struct {
	ProjectID       string
	TopicID         string
	CredentialsJSON string // JSON credentials as string
	CredentialsFile string // Path to credentials file

	client *pubsub.Client
	topic  *pubsub.Topic
	stats  PublishToGooglePubSubStats
	mu     sync.RWMutex
	ctx    context.Context
}

// PublishToGooglePubSubStats tracks publishing statistics
type PublishToGooglePubSubStats struct {
	TotalProcessed   uint64
	SuccessfulPublishes uint64
	FailedPublishes  uint64
}

// NewPublishToGooglePubSub creates a new Google Pub/Sub publisher consumer
func NewPublishToGooglePubSub(config map[string]interface{}) (*PublishToGooglePubSub, error) {
	consumer := &PublishToGooglePubSub{
		ProjectID:       getStringConfig(config, "project_id", ""),
		TopicID:         getStringConfig(config, "topic_id", ""),
		CredentialsJSON: getStringConfig(config, "credentials_json", ""),
		CredentialsFile: getStringConfig(config, "credentials_file", ""),
		ctx:             context.Background(),
	}

	// Support environment variables
	if envProjectID := os.Getenv("PUBSUB_PROJECT_ID"); envProjectID != "" {
		consumer.ProjectID = envProjectID
	}
	if envCredsJSON := os.Getenv("GCLOUD_PUBSUB_PUBLISHER_SERVICE_ACCOUNT_KEY"); envCredsJSON != "" {
		consumer.CredentialsJSON = envCredsJSON
	}
	if envCredsFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); envCredsFile != "" {
		consumer.CredentialsFile = envCredsFile
	}

	// Validate required config
	if consumer.ProjectID == "" {
		return nil, fmt.Errorf("project_id is required (or set PUBSUB_PROJECT_ID env var)")
	}
	if consumer.TopicID == "" {
		return nil, fmt.Errorf("topic_id is required")
	}

	log.Printf("PublishToGooglePubSub: Initializing Pub/Sub publisher for project=%s, topic=%s",
		consumer.ProjectID, consumer.TopicID)

	// Check if using emulator
	if emulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST"); emulatorHost != "" {
		log.Printf("PublishToGooglePubSub: Using Pub/Sub emulator at %s", emulatorHost)
	}

	// Create Pub/Sub client with credentials
	var clientOptions []option.ClientOption

	if consumer.CredentialsJSON != "" {
		// Use JSON credentials from env var or config
		clientOptions = append(clientOptions, option.WithCredentialsJSON([]byte(consumer.CredentialsJSON)))
		log.Printf("PublishToGooglePubSub: Using credentials from JSON string")
	} else if consumer.CredentialsFile != "" {
		// Use credentials file path
		clientOptions = append(clientOptions, option.WithCredentialsFile(consumer.CredentialsFile))
		log.Printf("PublishToGooglePubSub: Using credentials from file: %s", consumer.CredentialsFile)
	} else if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		// No credentials and not using emulator
		log.Printf("PublishToGooglePubSub: WARNING - No credentials provided, using default application credentials")
	}

	client, err := pubsub.NewClient(consumer.ctx, consumer.ProjectID, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	consumer.client = client

	// Get or create topic
	topic := client.Topic(consumer.TopicID)
	exists, err := topic.Exists(consumer.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if topic exists: %w", err)
	}

	if !exists {
		log.Printf("PublishToGooglePubSub: Topic %s does not exist, attempting to create it", consumer.TopicID)
		topic, err = client.CreateTopic(consumer.ctx, consumer.TopicID)
		if err != nil {
			return nil, fmt.Errorf("failed to create topic: %w", err)
		}
		log.Printf("PublishToGooglePubSub: Created topic %s", consumer.TopicID)
	} else {
		log.Printf("PublishToGooglePubSub: Topic %s exists", consumer.TopicID)
	}

	consumer.topic = topic

	log.Printf("PublishToGooglePubSub: Initialized successfully")

	return consumer, nil
}

// Process publishes EventPayment messages to Google Pub/Sub
func (c *PublishToGooglePubSub) Process(ctx context.Context, msg processor.Message) error {
	// Expect EventPayment payload
	eventPayment, ok := msg.Payload.(*processor.EventPayment)
	if !ok {
		return fmt.Errorf("expected *processor.EventPayment payload, got %T", msg.Payload)
	}

	c.mu.Lock()
	c.stats.TotalProcessed++
	c.mu.Unlock()

	// Convert EventPayment to JSON
	jsonData, err := json.Marshal(eventPayment)
	if err != nil {
		c.mu.Lock()
		c.stats.FailedPublishes++
		c.mu.Unlock()
		return fmt.Errorf("failed to marshal EventPayment to JSON: %w", err)
	}

	// Publish to Pub/Sub
	result := c.topic.Publish(ctx, &pubsub.Message{
		Data: jsonData,
		Attributes: map[string]string{
			"event_type":   "payment",
			"block_height": fmt.Sprintf("%d", eventPayment.BlockHeight),
			"payment_id":   eventPayment.PaymentID,
		},
	})

	// Block until the result is returned and a message ID is assigned
	messageID, err := result.Get(ctx)
	if err != nil {
		c.mu.Lock()
		c.stats.FailedPublishes++
		c.mu.Unlock()
		return fmt.Errorf("failed to publish to Pub/Sub: %w", err)
	}

	c.mu.Lock()
	c.stats.SuccessfulPublishes++
	c.mu.Unlock()

	log.Printf("PublishToGooglePubSub: Published payment_id=%s, block=%d, messageID=%s",
		eventPayment.PaymentID, eventPayment.BlockHeight, messageID)

	return nil
}

// Subscribe is not implemented for consumers (they are end of pipeline)
func (c *PublishToGooglePubSub) Subscribe(processor processor.Processor) {
	log.Printf("PublishToGooglePubSub: Subscribe called but consumers don't support subscriptions")
}

// GetStats returns current publishing statistics
func (c *PublishToGooglePubSub) GetStats() PublishToGooglePubSubStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

// Close closes the Pub/Sub client and topic, printing statistics
func (c *PublishToGooglePubSub) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("PublishToGooglePubSub Stats: Processed %d events, Published %d successfully, Failed %d",
		c.stats.TotalProcessed, c.stats.SuccessfulPublishes, c.stats.FailedPublishes)

	// Stop the topic (flush any pending messages)
	if c.topic != nil {
		log.Printf("PublishToGooglePubSub: Stopping topic to flush pending messages...")
		c.topic.Stop()
	}

	// Close the client
	if c.client != nil {
		log.Printf("PublishToGooglePubSub: Closing Pub/Sub client...")
		return c.client.Close()
	}

	return nil
}

// Ensure PublishToGooglePubSub implements Processor interface
var _ processor.Processor = (*PublishToGooglePubSub)(nil)
