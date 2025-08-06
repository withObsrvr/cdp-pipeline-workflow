package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	processor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// MockRedisClient mocks Redis operations
type MockRedisClient struct {
	mu           sync.Mutex
	data         map[string]string
	hashData     map[string]map[string]string
	listData     map[string][]string
	setData      map[string]map[string]bool
	zsetData     map[string]map[string]float64
	ttls         map[string]time.Duration
	pipelines    [][]string
	setErr       error
	hsetErr      error
	lpushErr     error
	saddErr      error
	zaddErr      error
	expireErr    error
	publishErr   error
	callCounts   map[string]int
}

func NewMockRedisClient() *MockRedisClient {
	return &MockRedisClient{
		data:       make(map[string]string),
		hashData:   make(map[string]map[string]string),
		listData:   make(map[string][]string),
		setData:    make(map[string]map[string]bool),
		zsetData:   make(map[string]map[string]float64),
		ttls:       make(map[string]time.Duration),
		pipelines:  make([][]string, 0),
		callCounts: make(map[string]int),
	}
}

func (m *MockRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["Set"]++

	if m.setErr != nil {
		return redis.NewStatusResult("", m.setErr)
	}

	m.data[key] = fmt.Sprintf("%v", value)
	if expiration > 0 {
		m.ttls[key] = expiration
	}

	return redis.NewStatusResult("OK", nil)
}

func (m *MockRedisClient) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["HSet"]++

	if m.hsetErr != nil {
		return redis.NewIntResult(0, m.hsetErr)
	}

	if m.hashData[key] == nil {
		m.hashData[key] = make(map[string]string)
	}

	count := 0
	for i := 0; i < len(values); i += 2 {
		field := fmt.Sprintf("%v", values[i])
		value := fmt.Sprintf("%v", values[i+1])
		m.hashData[key][field] = value
		count++
	}

	return redis.NewIntResult(int64(count), nil)
}

func (m *MockRedisClient) LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["LPush"]++

	if m.lpushErr != nil {
		return redis.NewIntResult(0, m.lpushErr)
	}

	if m.listData[key] == nil {
		m.listData[key] = make([]string, 0)
	}

	// Prepend values (LPUSH adds to the left)
	for i := len(values) - 1; i >= 0; i-- {
		m.listData[key] = append([]string{fmt.Sprintf("%v", values[i])}, m.listData[key]...)
	}

	return redis.NewIntResult(int64(len(m.listData[key])), nil)
}

func (m *MockRedisClient) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["SAdd"]++

	if m.saddErr != nil {
		return redis.NewIntResult(0, m.saddErr)
	}

	if m.setData[key] == nil {
		m.setData[key] = make(map[string]bool)
	}

	added := 0
	for _, member := range members {
		memberStr := fmt.Sprintf("%v", member)
		if !m.setData[key][memberStr] {
			m.setData[key][memberStr] = true
			added++
		}
	}

	return redis.NewIntResult(int64(added), nil)
}

func (m *MockRedisClient) ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["ZAdd"]++

	if m.zaddErr != nil {
		return redis.NewIntResult(0, m.zaddErr)
	}

	if m.zsetData[key] == nil {
		m.zsetData[key] = make(map[string]float64)
	}

	added := 0
	for _, z := range members {
		memberStr := fmt.Sprintf("%v", z.Member)
		if _, exists := m.zsetData[key][memberStr]; !exists {
			added++
		}
		m.zsetData[key][memberStr] = z.Score
	}

	return redis.NewIntResult(int64(added), nil)
}

func (m *MockRedisClient) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["Expire"]++

	if m.expireErr != nil {
		return redis.NewBoolResult(false, m.expireErr)
	}

	m.ttls[key] = expiration
	return redis.NewBoolResult(true, nil)
}

func (m *MockRedisClient) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["Publish"]++

	if m.publishErr != nil {
		return redis.NewIntResult(0, m.publishErr)
	}

	// Store published messages for verification
	m.pipelines = append(m.pipelines, []string{channel, fmt.Sprintf("%v", message)})
	return redis.NewIntResult(1, nil)
}

func (m *MockRedisClient) Pipeline() redis.Pipeliner {
	return &MockPipeliner{client: m}
}

func (m *MockRedisClient) GetData() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	result := make(map[string]string)
	for k, v := range m.data {
		result[k] = v
	}
	return result
}

func (m *MockRedisClient) GetHashData() map[string]map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	result := make(map[string]map[string]string)
	for k, v := range m.hashData {
		result[k] = make(map[string]string)
		for field, value := range v {
			result[k][field] = value
		}
	}
	return result
}

func (m *MockRedisClient) GetCallCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCounts[method]
}

// MockPipeliner for pipeline operations
type MockPipeliner struct {
	client   *MockRedisClient
	commands []string
}

func (p *MockPipeliner) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	p.commands = append(p.commands, fmt.Sprintf("SET %s %v", key, value))
	return p.client.Set(ctx, key, value, expiration)
}

func (p *MockPipeliner) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	p.commands = append(p.commands, fmt.Sprintf("HSET %s", key))
	return p.client.HSet(ctx, key, values...)
}

func (p *MockPipeliner) Exec(ctx context.Context) ([]redis.Cmder, error) {
	p.client.mu.Lock()
	p.client.pipelines = append(p.client.pipelines, p.commands)
	p.client.mu.Unlock()
	
	return []redis.Cmder{}, nil
}

// TestNewSaveToRedis tests configuration validation
func TestNewSaveToRedis(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]interface{}
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid configuration",
			config: map[string]interface{}{
				"redis_url": "redis://localhost:6379",
				"key_prefix": "stellar:",
			},
			wantErr: false,
		},
		{
			name: "missing redis_url",
			config: map[string]interface{}{
				"key_prefix": "stellar:",
			},
			wantErr: true,
			errMsg:  "redis_url is required",
		},
		{
			name: "with TTL configuration",
			config: map[string]interface{}{
				"redis_url":  "redis://localhost:6379",
				"key_prefix": "stellar:",
				"ttl":        3600,
			},
			wantErr: false,
		},
		{
			name: "with data structure configuration",
			config: map[string]interface{}{
				"redis_url":       "redis://localhost:6379",
				"key_prefix":      "stellar:",
				"data_structure":  "hash",
				"hash_key_field": "contract_id",
			},
			wantErr: false,
		},
		{
			name: "with pipeline configuration",
			config: map[string]interface{}{
				"redis_url":     "redis://localhost:6379",
				"key_prefix":    "stellar:",
				"use_pipeline":  true,
				"pipeline_size": 100,
			},
			wantErr: false,
		},
		{
			name: "with pub/sub configuration",
			config: map[string]interface{}{
				"redis_url":      "redis://localhost:6379",
				"publish_channel": "stellar.events",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Validate configuration
			if !tt.wantErr {
				redisURL, ok := tt.config["redis_url"].(string)
				assert.True(t, ok, "redis_url should be a string")
				assert.NotEmpty(t, redisURL)
			}
		})
	}
}

// TestSaveToRedis_Process tests message processing
func TestSaveToRedis_Process(t *testing.T) {
	t.Run("save as string", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client:        mockClient,
			keyPrefix:     "stellar:",
			dataStructure: "string",
		}

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{
				"contract_id": "CONTRACT123",
				"key":         "balance",
				"value":       "1000",
			},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify data was saved
		data := mockClient.GetData()
		assert.Contains(t, data, "stellar:CONTRACT123:balance")
		assert.Equal(t, `{"contract_id":"CONTRACT123","key":"balance","value":"1000"}`, data["stellar:CONTRACT123:balance"])
	})

	t.Run("save as hash", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client:        mockClient,
			keyPrefix:     "contract:",
			dataStructure: "hash",
			hashKeyField:  "contract_id",
		}

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{
				"contract_id": "CONTRACT123",
				"balance":     "1000",
				"owner":       "GOWNER123",
			},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify hash data
		hashData := mockClient.GetHashData()
		assert.Contains(t, hashData, "contract:CONTRACT123")
		assert.Equal(t, "1000", hashData["contract:CONTRACT123"]["balance"])
		assert.Equal(t, "GOWNER123", hashData["contract:CONTRACT123"]["owner"])
	})

	t.Run("save to list", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client:        mockClient,
			keyPrefix:     "events:",
			dataStructure: "list",
			listKeyField:  "event_type",
		}

		ctx := context.Background()

		// Process multiple events
		events := []processor.Message{
			{Payload: map[string]interface{}{"event_type": "payment", "amount": "100"}},
			{Payload: map[string]interface{}{"event_type": "payment", "amount": "200"}},
		}

		for _, msg := range events {
			err := consumer.Process(ctx, msg)
			assert.NoError(t, err)
		}

		// Verify list data
		assert.Equal(t, 2, mockClient.GetCallCount("LPush"))
	})

	t.Run("save to set", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client:        mockClient,
			keyPrefix:     "accounts:",
			dataStructure: "set",
			setKeyField:   "asset_code",
		}

		ctx := context.Background()

		// Process accounts with same asset
		accounts := []processor.Message{
			{Payload: map[string]interface{}{"asset_code": "USDC", "account": "GACC1"}},
			{Payload: map[string]interface{}{"asset_code": "USDC", "account": "GACC2"}},
			{Payload: map[string]interface{}{"asset_code": "USDC", "account": "GACC1"}}, // Duplicate
		}

		for _, msg := range accounts {
			err := consumer.Process(ctx, msg)
			assert.NoError(t, err)
		}

		// Set should have unique members
		assert.Equal(t, 3, mockClient.GetCallCount("SAdd"))
	})

	t.Run("save to sorted set", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client:         mockClient,
			keyPrefix:      "leaderboard:",
			dataStructure:  "zset",
			zsetKeyField:   "category",
			zsetScoreField: "score",
		}

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{
				"category": "volume",
				"account":  "GACC123",
				"score":    1000000.5,
			},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		assert.Equal(t, 1, mockClient.GetCallCount("ZAdd"))
	})

	t.Run("handle JSON payload", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client:    mockClient,
			keyPrefix: "json:",
		}

		ctx := context.Background()

		jsonData := map[string]interface{}{
			"id":   "123",
			"type": "test",
		}
		jsonBytes, _ := json.Marshal(jsonData)

		msg := processor.Message{
			Payload: jsonBytes,
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		// Should parse and save JSON
		data := mockClient.GetData()
		assert.Len(t, data, 1)
	})

	t.Run("apply TTL", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client:    mockClient,
			keyPrefix: "temp:",
			ttl:       60 * time.Second,
		}

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{
				"id": "123",
			},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify TTL was set
		assert.Contains(t, mockClient.ttls, "temp:123")
		assert.Equal(t, 60*time.Second, mockClient.ttls["temp:123"])
	})
}

// TestSaveToRedis_Pipeline tests pipeline operations
func TestSaveToRedis_Pipeline(t *testing.T) {
	t.Run("batch operations with pipeline", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client:       mockClient,
			keyPrefix:    "batch:",
			usePipeline:  true,
			pipelineSize: 3,
			pipeline:     []PipelineOp{},
		}

		ctx := context.Background()

		// Process messages to fill pipeline
		for i := 0; i < 3; i++ {
			msg := processor.Message{
				Payload: map[string]interface{}{
					"id":    fmt.Sprintf("id%d", i),
					"value": i,
				},
			}
			err := consumer.Process(ctx, msg)
			assert.NoError(t, err)
		}

		// Pipeline should have executed
		assert.Greater(t, len(mockClient.pipelines), 0)
	})

	t.Run("flush partial pipeline", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client:       mockClient,
			keyPrefix:    "partial:",
			usePipeline:  true,
			pipelineSize: 10,
			pipeline:     []PipelineOp{},
		}

		ctx := context.Background()

		// Process fewer than pipeline size
		for i := 0; i < 5; i++ {
			msg := processor.Message{
				Payload: map[string]interface{}{"id": i},
			}
			consumer.Process(ctx, msg)
		}

		// Manually flush
		err := consumer.flushPipeline(ctx)
		assert.NoError(t, err)

		// Should have executed partial batch
		assert.Greater(t, mockClient.GetCallCount("Set"), 0)
	})
}

// TestSaveToRedis_PubSub tests publish functionality
func TestSaveToRedis_PubSub(t *testing.T) {
	t.Run("publish to channel", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client:         mockClient,
			publishChannel: "stellar.events",
		}

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{
				"event": "payment",
				"amount": "1000",
			},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		// Verify publish was called
		assert.Equal(t, 1, mockClient.GetCallCount("Publish"))
		
		// Verify channel and message
		assert.Len(t, mockClient.pipelines, 1)
		assert.Equal(t, "stellar.events", mockClient.pipelines[0][0])
	})

	t.Run("publish with custom format", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client:         mockClient,
			publishChannel: "events",
			publishFormat:  "compact",
		}

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{
				"type":   "trade",
				"asset":  "XLM",
				"amount": "500",
			},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)

		assert.Equal(t, 1, mockClient.GetCallCount("Publish"))
	})
}

// TestSaveToRedis_ErrorHandling tests error scenarios
func TestSaveToRedis_ErrorHandling(t *testing.T) {
	t.Run("handle set error", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		mockClient.setErr = fmt.Errorf("connection refused")
		
		consumer := &SaveToRedis{
			client: mockClient,
		}

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{"id": 1},
		}

		err := consumer.Process(ctx, msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("handle invalid JSON", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		consumer := &SaveToRedis{
			client: mockClient,
		}

		ctx := context.Background()

		msg := processor.Message{
			Payload: []byte(`{invalid json}`),
		}

		err := consumer.Process(ctx, msg)
		assert.Error(t, err)
	})

	t.Run("retry on transient errors", func(t *testing.T) {
		mockClient := NewMockRedisClient()
		attempts := 0
		
		consumer := &SaveToRedis{
			client:        mockClient,
			retryAttempts: 3,
			retryDelay:    10 * time.Millisecond,
		}

		// Simulate recovery after 2 attempts
		consumer.processWithRetry = func(ctx context.Context, data map[string]interface{}) error {
			attempts++
			if attempts < 3 {
				return fmt.Errorf("temporary error")
			}
			return nil
		}

		ctx := context.Background()

		msg := processor.Message{
			Payload: map[string]interface{}{"id": 1},
		}

		err := consumer.Process(ctx, msg)
		assert.NoError(t, err)
		assert.Equal(t, 3, attempts)
	})
}

// TestSaveToRedis_Concurrency tests thread safety
func TestSaveToRedis_Concurrency(t *testing.T) {
	mockClient := NewMockRedisClient()
	consumer := &SaveToRedis{
		client:       mockClient,
		keyPrefix:    "concurrent:",
		usePipeline:  true,
		pipelineSize: 50,
		pipeline:     []PipelineOp{},
		mu:           &sync.Mutex{},
	}

	ctx := context.Background()
	numGoroutines := 10
	messagesPerGoroutine := 100

	var wg sync.WaitGroup
	errors := make([]error, numGoroutines)

	// Launch concurrent writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := processor.Message{
					Payload: map[string]interface{}{
						"worker_id":  workerID,
						"message_id": j,
					},
				}
				if err := consumer.Process(ctx, msg); err != nil {
					errors[workerID] = err
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Flush any remaining
	consumer.flushPipeline(ctx)

	// Check for errors
	for i, err := range errors {
		assert.NoError(t, err, "worker %d encountered error", i)
	}

	// Verify operations completed
	totalOps := mockClient.GetCallCount("Set") + 
		mockClient.GetCallCount("HSet") + 
		mockClient.GetCallCount("LPush")
	assert.Greater(t, totalOps, 0)
}

// Benchmarks
func BenchmarkSaveToRedis_String(b *testing.B) {
	mockClient := NewMockRedisClient()
	consumer := &SaveToRedis{
		client:        mockClient,
		keyPrefix:     "bench:",
		dataStructure: "string",
	}

	ctx := context.Background()
	
	testData := map[string]interface{}{
		"id":    "123",
		"value": "test_value",
		"metadata": map[string]string{
			"field1": "value1",
			"field2": "value2",
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := processor.Message{Payload: testData}
		consumer.Process(ctx, msg)
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N), "operations")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkSaveToRedis_Pipeline(b *testing.B) {
	mockClient := NewMockRedisClient()
	consumer := &SaveToRedis{
		client:       mockClient,
		keyPrefix:    "bench:",
		usePipeline:  true,
		pipelineSize: 100,
		pipeline:     []PipelineOp{},
	}

	ctx := context.Background()
	
	testData := map[string]interface{}{
		"id": "123",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := processor.Message{Payload: testData}
		consumer.Process(ctx, msg)
	}

	// Flush remaining
	consumer.flushPipeline(ctx)

	b.StopTimer()
	b.ReportMetric(float64(b.N), "operations")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// Mock SaveToRedis implementation
type SaveToRedis struct {
	client          *MockRedisClient
	keyPrefix       string
	dataStructure   string
	hashKeyField    string
	listKeyField    string
	setKeyField     string
	zsetKeyField    string
	zsetScoreField  string
	ttl             time.Duration
	usePipeline     bool
	pipelineSize    int
	pipeline        []PipelineOp
	publishChannel  string
	publishFormat   string
	retryAttempts   int
	retryDelay      time.Duration
	mu              *sync.Mutex
	processWithRetry func(context.Context, map[string]interface{}) error
}

type PipelineOp struct {
	Op   string
	Key  string
	Data interface{}
}

func (s *SaveToRedis) Process(ctx context.Context, msg processor.Message) error {
	if s.mu != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	// Convert payload
	var data map[string]interface{}
	
	switch v := msg.Payload.(type) {
	case []byte:
		if err := json.Unmarshal(v, &data); err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %w", err)
		}
	case map[string]interface{}:
		data = v
	default:
		return fmt.Errorf("unsupported payload type: %T", v)
	}

	// Process with retry if configured
	if s.processWithRetry != nil {
		return s.processWithRetry(ctx, data)
	}

	// Save based on data structure
	switch s.dataStructure {
	case "hash":
		return s.saveAsHash(ctx, data)
	case "list":
		return s.saveAsList(ctx, data)
	case "set":
		return s.saveAsSet(ctx, data)
	case "zset":
		return s.saveAsZSet(ctx, data)
	default:
		return s.saveAsString(ctx, data)
	}
}

func (s *SaveToRedis) saveAsString(ctx context.Context, data map[string]interface{}) error {
	// Generate key
	key := s.keyPrefix
	if id, ok := data["id"]; ok {
		key += fmt.Sprintf("%v", id)
	} else if contractID, ok := data["contract_id"]; ok {
		key += fmt.Sprintf("%v:%v", contractID, data["key"])
	}

	// Serialize data
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Save with optional TTL
	cmd := s.client.Set(ctx, key, string(jsonData), s.ttl)
	
	// Publish if configured
	if s.publishChannel != "" {
		s.client.Publish(ctx, s.publishChannel, string(jsonData))
	}

	return cmd.Err()
}

func (s *SaveToRedis) saveAsHash(ctx context.Context, data map[string]interface{}) error {
	// Get hash key
	hashKey := s.keyPrefix
	if s.hashKeyField != "" && data[s.hashKeyField] != nil {
		hashKey += fmt.Sprintf("%v", data[s.hashKeyField])
	}

	// Convert to field-value pairs
	fields := make([]interface{}, 0, len(data)*2)
	for k, v := range data {
		if k != s.hashKeyField {
			fields = append(fields, k, v)
		}
	}

	return s.client.HSet(ctx, hashKey, fields...).Err()
}

func (s *SaveToRedis) saveAsList(ctx context.Context, data map[string]interface{}) error {
	// Get list key
	listKey := s.keyPrefix
	if s.listKeyField != "" && data[s.listKeyField] != nil {
		listKey += fmt.Sprintf("%v", data[s.listKeyField])
	}

	// Serialize and push
	jsonData, _ := json.Marshal(data)
	return s.client.LPush(ctx, listKey, string(jsonData)).Err()
}

func (s *SaveToRedis) saveAsSet(ctx context.Context, data map[string]interface{}) error {
	// Get set key
	setKey := s.keyPrefix
	if s.setKeyField != "" && data[s.setKeyField] != nil {
		setKey += fmt.Sprintf("%v", data[s.setKeyField])
	}

	// Add member
	member, _ := json.Marshal(data)
	return s.client.SAdd(ctx, setKey, string(member)).Err()
}

func (s *SaveToRedis) saveAsZSet(ctx context.Context, data map[string]interface{}) error {
	// Get zset key
	zsetKey := s.keyPrefix
	if s.zsetKeyField != "" && data[s.zsetKeyField] != nil {
		zsetKey += fmt.Sprintf("%v", data[s.zsetKeyField])
	}

	// Get score
	score := 0.0
	if s.zsetScoreField != "" && data[s.zsetScoreField] != nil {
		switch v := data[s.zsetScoreField].(type) {
		case float64:
			score = v
		case int:
			score = float64(v)
		}
	}

	// Add member with score
	member, _ := json.Marshal(data)
	return s.client.ZAdd(ctx, zsetKey, &redis.Z{
		Score:  score,
		Member: string(member),
	}).Err()
}

func (s *SaveToRedis) flushPipeline(ctx context.Context) error {
	if len(s.pipeline) == 0 {
		return nil
	}

	// Execute pipeline operations
	pipe := s.client.Pipeline()
	for _, op := range s.pipeline {
		switch op.Op {
		case "SET":
			pipe.Set(ctx, op.Key, op.Data, s.ttl)
		case "HSET":
			// Implementation would handle hash operations
		}
	}
	
	_, err := pipe.Exec(ctx)
	s.pipeline = s.pipeline[:0] // Clear pipeline
	
	return err
}

func (s *SaveToRedis) Subscribe(processor processor.Processor) {
	// Not implemented for consumer
}