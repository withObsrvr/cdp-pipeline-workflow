package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type SaveToMongoDB struct {
	client     *mongo.Client
	collection *mongo.Collection
	processors []processor.Processor
}

type MongoDBConfig struct {
	URI            string
	Database       string
	Collection     string
	ConnectTimeout time.Duration
}

func NewSaveToMongoDB(config map[string]interface{}) (*SaveToMongoDB, error) {
	// Extract configuration
	dbConfig, err := parseMongoDBConfig(config)
	if err != nil {
		return nil, err
	}

	// Create MongoDB client
	ctx, cancel := context.WithTimeout(context.Background(), dbConfig.ConnectTimeout)
	defer cancel()

	// Set client options
	clientOptions := options.Client().
		ApplyURI(dbConfig.URI)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create MongoDB client: %v", err)
	}

	// Verify connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	// Get collection reference
	collection := client.Database(dbConfig.Database).Collection(dbConfig.Collection)

	log.Printf("Successfully connected to MongoDB database %s, collection %s",
		dbConfig.Database, dbConfig.Collection)

	return &SaveToMongoDB{
		client:     client,
		collection: collection,
	}, nil
}

func parseMongoDBConfig(config map[string]interface{}) (MongoDBConfig, error) {
	var dbConfig MongoDBConfig

	uri, ok := config["uri"].(string)
	if !ok {
		return dbConfig, fmt.Errorf("missing 'uri' in MongoDB configuration")
	}
	dbConfig.URI = uri

	database, ok := config["database"].(string)
	if !ok {
		return dbConfig, fmt.Errorf("missing 'database' in MongoDB configuration")
	}
	dbConfig.Database = database

	collection, ok := config["collection"].(string)
	if !ok {
		return dbConfig, fmt.Errorf("missing 'collection' in MongoDB configuration")
	}
	dbConfig.Collection = collection

	// Set default timeout if not provided
	timeout, ok := config["connect_timeout"].(int)
	if !ok {
		timeout = 10 // Default 10 seconds
	}
	dbConfig.ConnectTimeout = time.Duration(timeout) * time.Second

	return dbConfig, nil
}

func (m *SaveToMongoDB) Subscribe(processor processor.Processor) {
	m.processors = append(m.processors, processor)
}

func (m *SaveToMongoDB) Process(ctx context.Context, msg processor.Message) error {

	// Convert payload to []byte
	payloadBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte payload, got %T", msg.Payload)
	}

	// First unmarshal the payload bytes into an AppPayment struct
	var payment AppPayment
	if err := json.Unmarshal(payloadBytes, &payment); err != nil {
		return fmt.Errorf("failed to unmarshal payload to AppPayment: %v", err)
	}

	// First try to unmarshal as a generic map to get the type
	var rawDoc map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &rawDoc); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %v", err)
	}

	operationType, ok := rawDoc["type"].(string)
	if !ok {
		return fmt.Errorf("missing operation type in payload")
	}

	var doc bson.D
	switch operationType {
	case "payment":
		var payment AppPayment
		if err := json.Unmarshal(payloadBytes, &payment); err != nil {
			return fmt.Errorf("failed to unmarshal payment: %v", err)
		}
		doc = bson.D{
			{Key: "timestamp", Value: payment.Timestamp},
			{Key: "buyer_account_id", Value: payment.BuyerAccountId},
			{Key: "seller_account_id", Value: payment.SellerAccountId},
			{Key: "asset_code", Value: payment.AssetCode},
			{Key: "amount", Value: payment.Amount},
			{Key: "type", Value: payment.Type},
			{Key: "memo", Value: payment.Memo},
			{Key: "created_at", Value: time.Now().UTC()},
		}

	case "create_account":
		var createAccount CreateAccountOp
		if err := json.Unmarshal(payloadBytes, &createAccount); err != nil {
			return fmt.Errorf("failed to unmarshal create account: %v", err)
		}
		doc = bson.D{
			{Key: "timestamp", Value: createAccount.Timestamp},
			{Key: "funder", Value: createAccount.Funder},
			{Key: "account", Value: createAccount.Account},
			{Key: "starting_balance", Value: createAccount.StartingBalance},
			{Key: "type", Value: createAccount.Type},
			{Key: "created_at", Value: time.Now().UTC()},
		}

	default:
		return fmt.Errorf("unknown operation type: %s", operationType)
	}

	// Insert document with timeout
	insertCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	result, err := m.collection.InsertOne(insertCtx, doc)
	if err != nil {
		return fmt.Errorf("failed to insert document: %v", err)
	}

	log.Printf("Successfully inserted document with ID: %v", result.InsertedID)
	return nil
}

func (m *SaveToMongoDB) Close() error {
	if m.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return m.client.Disconnect(ctx)
	}
	return nil
}
