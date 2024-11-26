package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"google.golang.org/api/option"
)

type SaveToGCS struct {
	bucketName    string
	objectPrefix  string
	projectID     string
	client        *storage.Client
	processors    []processor.Processor
	writer        *storage.Writer
	mutex         sync.Mutex
	buffer        []byte
	bufferSize    int
	currentTime   time.Time
	lastFlushTime time.Time
}

func NewSaveToGCS(config map[string]interface{}) (*SaveToGCS, error) {
	bucketName, ok := config["bucket_name"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration: missing 'bucket_name'")
	}

	objectPrefix, ok := config["object_prefix"].(string)
	if !ok {
		objectPrefix = "stellar-payments"
	}

	projectID, ok := config["project_id"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration: missing 'project_id'")
	}

	bufferSize, ok := config["buffer_size"].(int)
	if !ok {
		bufferSize = 1 << 20 // 1 MB default
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile("/home/tmosleyiii/.config/gcloud/application_default_credentials.json"))
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %v", err)
	}

	saver := &SaveToGCS{
		bucketName:    bucketName,
		objectPrefix:  objectPrefix,
		projectID:     projectID,
		client:        client,
		bufferSize:    bufferSize,
		buffer:        make([]byte, 0, bufferSize),
		currentTime:   time.Now().UTC(),
		lastFlushTime: time.Now(),
	}

	// Start periodic flush goroutine
	go saver.startPeriodicFlush(ctx)

	return saver, nil
}

func (g *SaveToGCS) getObjectName() string {
	timestamp := g.currentTime.Format("2006/01/02/15")
	return filepath.Join(g.objectPrefix, timestamp, fmt.Sprintf("payments-%s.ndjson",
		g.currentTime.Format("150405")))
}

func (g *SaveToGCS) rotateWriter(ctx context.Context) error {
	if g.writer != nil {
		if err := g.writer.Close(); err != nil {
			return fmt.Errorf("failed to close previous writer: %v", err)
		}
	}

	// Update current time and create new writer
	g.currentTime = time.Now().UTC()
	bucket := g.client.Bucket(g.bucketName)
	obj := bucket.Object(g.getObjectName())

	g.writer = obj.NewWriter(ctx)
	g.writer.ContentType = "application/x-ndjson"
	g.writer.Metadata = map[string]string{
		"content-type": "stellar-payments",
		"created-at":   g.currentTime.Format(time.RFC3339),
	}

	return nil
}

func (g *SaveToGCS) Subscribe(processor processor.Processor) {
	g.processors = append(g.processors, processor)
}

func (g *SaveToGCS) Process(ctx context.Context, msg processor.Message) error {
	log.Println("Processing message in SaveToGCS")
	g.mutex.Lock()
	defer g.mutex.Unlock()

	// Create JSON line from payload
	line, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %v", err)
	}
	line = append(line, '\n')

	// Initialize or rotate writer if needed
	if g.writer == nil {
		if err := g.rotateWriter(ctx); err != nil {
			return err
		}
	} else if time.Now().UTC().Hour() != g.currentTime.Hour() {
		if err := g.rotateWriter(ctx); err != nil {
			return err
		}
	}

	log.Println("Appending line to buffer in SaveToGCS")

	g.buffer = append(g.buffer, line...)

	log.Printf("Buffer size: %d, Last flush time: %v", len(g.buffer), g.lastFlushTime)
	if len(g.buffer) >= g.bufferSize || time.Since(g.lastFlushTime) > 5*time.Minute {
		log.Println("Flushing buffer")
		if err := g.flushBuffer(ctx); err != nil {
			log.Printf("Error flushing buffer: %v", err)
			return err
		}
		log.Println("Buffer flushed successfully")
		g.lastFlushTime = time.Now()
	}

	return nil
}

func (g *SaveToGCS) flushBuffer(ctx context.Context) error {
	if len(g.buffer) == 0 {
		return nil
	}

	if g.writer == nil {
		if err := g.rotateWriter(ctx); err != nil {
			return err
		}
	}

	_, err := g.writer.Write(g.buffer)
	if err != nil {
		return fmt.Errorf("failed to write to GCS: %v", err)
	}

	g.buffer = g.buffer[:0]
	return nil
}

func (g *SaveToGCS) startPeriodicFlush(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Println("Flushing buffer in SaveToGCS")
			g.mutex.Lock()
			if err := g.flushBuffer(ctx); err != nil {
				log.Printf("Error flushing buffer: %v", err)
			}
			g.mutex.Unlock()
		}
	}
}

func (g *SaveToGCS) Close() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if err := g.flushBuffer(context.Background()); err != nil {
		return err
	}

	if g.writer != nil {
		if err := g.writer.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %v", err)
		}
	}

	return g.client.Close()
}
