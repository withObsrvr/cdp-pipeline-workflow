package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"google.golang.org/api/option"
)

// ChainIdentifier represents the Stellar network identifier
type ChainIdentifier string

const (
	ChainIdentifierStellarMainnet ChainIdentifier = "StellarMainnet"
	ChainIdentifierStellarTestnet ChainIdentifier = "StellarTestnet"
)

const (
	// Pub/Sub message attributes
	EventTypePayment   = "payment"
	MessageVersionV2   = "v2"
	AttributeEventType = "event_type"
	AttributeChainID   = "chain_identifier"
	AttributeBlockHeight = "block_height"
	AttributePaymentID = "payment_id"
	AttributeMessageVersion = "message_version"
)

// PubSubPayloadV2 represents the payment data payload in the new format
type PubSubPayloadV2 struct {
	PaymentID        string `json:"paymentId"`
	MerchantAddress  string `json:"merchantAddress"`
	Amount           string `json:"amount"`
	RoyaltyFee       string `json:"royaltyFee"`
	PayerAddress     string `json:"payerAddress"`
}

// PubSubDetailsV2 represents transaction details in the new format
type PubSubDetailsV2 struct {
	Hash  string `json:"hash"`
	Block uint32 `json:"block"`
	To    string `json:"to"`
	From  string `json:"from"`
}

// PubSubMessageV2 is the top-level message structure for V2 format
type PubSubMessageV2 struct {
	ChainIdentifier ChainIdentifier  `json:"chainIdentifier"`
	Payload         PubSubPayloadV2  `json:"payload"`
	Details         PubSubDetailsV2  `json:"details"`
}

// PublishToGooglePubSubV2 publishes EventPayment messages to Google Pub/Sub in V2 format
// This consumer formats messages to match the structure expected by downstream services:
// {
//   "chainIdentifier": "StellarMainnet" | "StellarTestnet",
//   "payload": { paymentId, merchantAddress, amount, royaltyFee, payerAddress },
//   "details": { hash, block, to, from }
// }
type PublishToGooglePubSubV2 struct {
	ProjectID       string
	TopicID         string
	ChainIdentifier ChainIdentifier
	CredentialsJSON string // JSON credentials as string
	CredentialsFile string // Path to credentials file

	client *pubsub.Client
	topic  *pubsub.Topic
	stats  PublishToGooglePubSubV2Stats
}

// PublishToGooglePubSubV2Stats tracks publishing statistics using atomic operations
type PublishToGooglePubSubV2Stats struct {
	TotalProcessed      atomic.Uint64
	SuccessfulPublishes atomic.Uint64
	FailedPublishes     atomic.Uint64
}

// NewPublishToGooglePubSubV2 creates a new Google Pub/Sub publisher consumer with V2 message format
func NewPublishToGooglePubSubV2(config map[string]interface{}) (*PublishToGooglePubSubV2, error) {
	consumer := &PublishToGooglePubSubV2{
		ProjectID:       getStringConfig(config, "project_id", ""),
		TopicID:         getStringConfig(config, "topic_id", ""),
		ChainIdentifier: ChainIdentifier(getStringConfig(config, "chain_identifier", string(ChainIdentifierStellarTestnet))),
		CredentialsJSON: getStringConfig(config, "credentials_json", ""),
		CredentialsFile: getStringConfig(config, "credentials_file", ""),
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
	if envChainID := os.Getenv("CHAIN_IDENTIFIER"); envChainID != "" {
		consumer.ChainIdentifier = ChainIdentifier(envChainID)
	}

	// Validate chain identifier
	if consumer.ChainIdentifier != ChainIdentifierStellarMainnet && consumer.ChainIdentifier != ChainIdentifierStellarTestnet {
		return nil, fmt.Errorf("invalid chain_identifier: %s (must be %s or %s)",
			consumer.ChainIdentifier, ChainIdentifierStellarMainnet, ChainIdentifierStellarTestnet)
	}

	// Validate required config
	if consumer.ProjectID == "" {
		return nil, fmt.Errorf("project_id is required (or set PUBSUB_PROJECT_ID env var)")
	}
	if consumer.TopicID == "" {
		return nil, fmt.Errorf("topic_id is required")
	}

	log.Printf("PublishToGooglePubSubV2: Initializing Pub/Sub publisher for project=%s, topic=%s, chain=%s",
		consumer.ProjectID, consumer.TopicID, consumer.ChainIdentifier)

	// Check if using emulator
	if emulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST"); emulatorHost != "" {
		log.Printf("PublishToGooglePubSubV2: Using Pub/Sub emulator at %s", emulatorHost)
	}

	// Create Pub/Sub client with credentials
	var clientOptions []option.ClientOption

	if consumer.CredentialsJSON != "" {
		// Use JSON credentials from env var or config
		clientOptions = append(clientOptions, option.WithCredentialsJSON([]byte(consumer.CredentialsJSON)))
		log.Printf("PublishToGooglePubSubV2: Using credentials from JSON string")
	} else if consumer.CredentialsFile != "" {
		// Use credentials file path
		clientOptions = append(clientOptions, option.WithCredentialsFile(consumer.CredentialsFile))
		log.Printf("PublishToGooglePubSubV2: Using credentials from file: %s", consumer.CredentialsFile)
	} else if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		// No credentials and not using emulator
		log.Printf("PublishToGooglePubSubV2: WARNING - No credentials provided, using default application credentials")
	}

	// Use background context for initialization
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, consumer.ProjectID, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	consumer.client = client

	// Get or create topic
	topic := client.Topic(consumer.TopicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if topic exists: %w", err)
	}

	if !exists {
		log.Printf("PublishToGooglePubSubV2: Topic %s does not exist, attempting to create it", consumer.TopicID)
		topic, err = client.CreateTopic(ctx, consumer.TopicID)
		if err != nil {
			return nil, fmt.Errorf("failed to create topic: %w", err)
		}
		log.Printf("PublishToGooglePubSubV2: Created topic %s", consumer.TopicID)
	} else {
		log.Printf("PublishToGooglePubSubV2: Topic %s exists", consumer.TopicID)
	}

	consumer.topic = topic

	log.Printf("PublishToGooglePubSubV2: Initialized successfully")

	return consumer, nil
}

// Process publishes EventPayment messages to Google Pub/Sub in V2 format
func (c *PublishToGooglePubSubV2) Process(ctx context.Context, msg processor.Message) error {
	// Expect EventPayment payload
	eventPayment, ok := msg.Payload.(*processor.EventPayment)
	if !ok {
		return fmt.Errorf("expected *processor.EventPayment payload, got %T", msg.Payload)
	}

	c.stats.TotalProcessed.Add(1)

	// Build V2 message structure
	v2Message := PubSubMessageV2{
		ChainIdentifier: c.ChainIdentifier,
		Payload: PubSubPayloadV2{
			PaymentID:       eventPayment.PaymentID,
			MerchantAddress: eventPayment.MerchantID,
			Amount:          strconv.FormatUint(eventPayment.Amount, 10),
			RoyaltyFee:      strconv.FormatUint(eventPayment.RoyaltyAmount, 10),
			PayerAddress:    eventPayment.FromID,
		},
		Details: PubSubDetailsV2{
			Hash:  eventPayment.TxHash,
			Block: eventPayment.BlockHeight,
			To:    eventPayment.MerchantID,
			From:  eventPayment.FromID,
		},
	}

	// Convert V2 message to JSON
	jsonData, err := json.Marshal(v2Message)
	if err != nil {
		c.stats.FailedPublishes.Add(1)
		return fmt.Errorf("failed to marshal V2 message to JSON: %w", err)
	}

	// Publish to Pub/Sub
	result := c.topic.Publish(ctx, &pubsub.Message{
		Data: jsonData,
		Attributes: map[string]string{
			AttributeEventType:      EventTypePayment,
			AttributeChainID:        string(c.ChainIdentifier),
			AttributeBlockHeight:    fmt.Sprintf("%d", eventPayment.BlockHeight),
			AttributePaymentID:      eventPayment.PaymentID,
			AttributeMessageVersion: MessageVersionV2,
		},
	})

	// Block until the result is returned and a message ID is assigned
	messageID, err := result.Get(ctx)
	if err != nil {
		c.stats.FailedPublishes.Add(1)
		return fmt.Errorf("failed to publish to Pub/Sub: %w", err)
	}

	c.stats.SuccessfulPublishes.Add(1)

	log.Printf("PublishToGooglePubSubV2: Published payment_id=%s, chain=%s, block=%d, messageID=%s",
		eventPayment.PaymentID, c.ChainIdentifier, eventPayment.BlockHeight, messageID)

	return nil
}

// Subscribe is not implemented for consumers (they are end of pipeline)
func (c *PublishToGooglePubSubV2) Subscribe(processor processor.Processor) {
	log.Printf("PublishToGooglePubSubV2: Subscribe called but consumers don't support subscriptions")
}

// GetStats returns current publishing statistics
// Note: With atomic operations, we can return stats without locking
func (c *PublishToGooglePubSubV2) GetStats() PublishToGooglePubSubV2Stats {
	return c.stats
}

// Close closes the Pub/Sub client and topic, printing statistics
func (c *PublishToGooglePubSubV2) Close() error {
	// Read stats (atomic operations are thread-safe)
	log.Printf("PublishToGooglePubSubV2 Stats: Processed %d events, Published %d successfully, Failed %d",
		c.stats.TotalProcessed.Load(), c.stats.SuccessfulPublishes.Load(), c.stats.FailedPublishes.Load())

	// Stop the topic (flush any pending messages)
	if c.topic != nil {
		log.Printf("PublishToGooglePubSubV2: Stopping topic to flush pending messages...")
		c.topic.Stop()
	}

	// Close the client
	if c.client != nil {
		log.Printf("PublishToGooglePubSubV2: Closing Pub/Sub client...")
		return c.client.Close()
	}

	return nil
}

// Ensure PublishToGooglePubSubV2 implements Processor interface
var _ processor.Processor = (*PublishToGooglePubSubV2)(nil)
