package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

type AssetStats struct {
	Timestamp   time.Time
	AssetCode   string
	AssetIssuer string
	AssetType   string
	Type        string // Add this field for operation type

	AccountsAuthorized      uint64
	AccountsAuthLiabilities uint64
	AccountsUnauthorized    uint64

	BalanceAuthorized      float64
	BalanceAuthLiabilities float64
	BalanceUnauthorized    float64

	NumClaimableBalances uint64
	NumContracts         uint64
	NumLiquidityPools    uint64

	AuthRequired      bool
	AuthRevocable     bool
	AuthImmutable     bool
	AuthClawback      bool
	TotalSupply       float64 `json:"total_supply"`
	CirculatingSupply float64 `json:"circulating_supply"`
	NumHolders        uint64  `json:"num_holders"` // Active trustlines
}

type AssetStatsCache struct {
	mu    sync.RWMutex
	stats map[string]*AssetStats // key: assetCode:issuer
}

func NewAssetStatsCache() *AssetStatsCache {
	return &AssetStatsCache{
		stats: make(map[string]*AssetStats),
	}
}

func (c *AssetStatsCache) GetOrCreate(assetCode, assetIssuer string) *AssetStats {
	// Clean asset code by removing null bytes
	cleanCode := strings.TrimRight(assetCode, "\x00")
	key := fmt.Sprintf("%s:%s", cleanCode, assetIssuer)

	c.mu.RLock()
	if stats, exists := c.stats[key]; exists {
		c.mu.RUnlock()
		return stats
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if stats, exists := c.stats[key]; exists {
		return stats
	}

	stats := &AssetStats{
		Timestamp:            time.Now(),
		AssetCode:            cleanCode,
		AssetIssuer:          assetIssuer,
		Type:                 "asset_stats",
		AssetType:            getAssetType(cleanCode),
		AccountsAuthorized:   0,
		AccountsUnauthorized: 0,
		BalanceAuthorized:    0,
		BalanceUnauthorized:  0,
	}
	c.stats[key] = stats
	return stats
}

type TransformToAssetStats struct {
	networkPassphrase string
	processors        []Processor
	cache             *AssetStatsCache
	redisClient       *redis.Client
}

func NewTransformToAssetStats(config map[string]interface{}) (*TransformToAssetStats, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration: missing 'network_passphrase'")
	}
	redisAddress, ok := config["redis_address"].(string)
	if !ok {
		return nil, fmt.Errorf("missing redis_address in configuration")
	}
	redisPassword, ok := config["redis_password"].(string)
	if !ok {
		return nil, fmt.Errorf("missing redis_password in configuration")
	}
	redisDB, ok := config["redis_db"].(int)
	if !ok {
		return nil, fmt.Errorf("missing redis_db in configuration")
	}

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: redisPassword,
		DB:       redisDB,
	})

	// Test connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	return &TransformToAssetStats{
		networkPassphrase: networkPassphrase,
		cache:             NewAssetStatsCache(),
		redisClient:       redisClient,
	}, nil
}

func (t *TransformToAssetStats) Subscribe(processor Processor) {
	t.processors = append(t.processors, processor)
}

func (t *TransformToAssetStats) Process(ctx context.Context, msg Message) error {
	var ledgerCloseMeta xdr.LedgerCloseMeta
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(t.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("failed to create reader for ledger %v: %w", ledgerCloseMeta.LedgerSequence(), err)
	}
	defer ledgerTxReader.Close()

	closeTime := uint(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)

	for {
		tx, err := ledgerTxReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Process each operation in the transaction
		for _, op := range tx.Envelope.Operations() {
			switch op.Body.Type {
			case xdr.OperationTypeChangeTrust:
				if err := t.processChangeTrust(ctx, op.Body.ChangeTrustOp, closeTime); err != nil {
					return fmt.Errorf("error processing change trust: %w", err)
				}
			case xdr.OperationTypePayment:
				if err := t.processPayment(ctx, op.Body.PaymentOp, closeTime); err != nil {
					return fmt.Errorf("error processing payment: %w", err)
				}
			case xdr.OperationTypeCreateClaimableBalance:
				if err := t.processClaimableBalance(ctx, op.Body.CreateClaimableBalanceOp, closeTime); err != nil {
					return fmt.Errorf("error processing claimable balance: %w", err)
				}
			}
		}
	}

	// Forward stats with proper type information
	t.cache.mu.RLock()
	defer t.cache.mu.RUnlock()

	for _, stats := range t.cache.stats {
		// Ensure Type and AssetType are set
		stats.Type = "asset_stats"
		stats.AssetType = getAssetType(stats.AssetCode)

		// Convert to map for consistent payload structure
		statsMap := map[string]interface{}{
			"operation_type":        stats.Type,
			"type":                  stats.Type,
			"asset_type":            stats.AssetType,
			"asset_code":            strings.TrimRight(stats.AssetCode, "\x00"),
			"asset_issuer":          stats.AssetIssuer,
			"circulating_supply":    stats.CirculatingSupply,
			"total_supply":          stats.TotalSupply,
			"num_holders":           stats.NumHolders,
			"accounts_authorized":   stats.AccountsAuthorized,
			"accounts_unauthorized": stats.AccountsUnauthorized,
			"balance_authorized":    stats.BalanceAuthorized,
			"balance_unauthorized":  stats.BalanceUnauthorized,
			"timestamp":             stats.Timestamp.Format(time.RFC3339),
		}

		jsonBytes, err := json.Marshal(statsMap)
		if err != nil {
			return fmt.Errorf("error marshaling stats: %w", err)
		}

		for _, processor := range t.processors {
			if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
				return fmt.Errorf("error in processor chain: %w", err)
			}
		}
	}

	return nil
}

func (t *TransformToAssetStats) processChangeTrust(ctx context.Context, op *xdr.ChangeTrustOp, closeTime uint) error {
	asset := op.Line.ToAsset()
	code := asset.GetCode()
	issuer := asset.GetIssuer()

	stats := t.cache.GetOrCreate(code, issuer)
	stats.Timestamp = time.Unix(int64(closeTime), 0)

	// Update trustline stats based on operation
	if op.Limit != 0 {
		stats.AccountsAuthorized++
		stats.NumHolders++
	} else {
		stats.AccountsAuthorized--
		stats.NumHolders--
	}

	return nil
}

func (t *TransformToAssetStats) processPayment(ctx context.Context, op *xdr.PaymentOp, closeTime uint) error {
	asset := op.Asset
	code := asset.GetCode()
	issuer := asset.GetIssuer()

	stats := t.cache.GetOrCreate(code, issuer)
	stats.Timestamp = time.Unix(int64(closeTime), 0)

	// Update circulating supply
	amount := float64(op.Amount) / 10000000 // Convert from stroops
	stats.CirculatingSupply += amount
	stats.TotalSupply += amount

	return nil
}

func (t *TransformToAssetStats) processClaimableBalance(ctx context.Context, op *xdr.CreateClaimableBalanceOp, closeTime uint) error {
	asset := op.Asset
	code := asset.GetCode()
	issuer := asset.GetIssuer()

	stats := t.cache.GetOrCreate(code, issuer)
	stats.NumClaimableBalances++
	stats.Timestamp = time.Unix(int64(closeTime), 0)

	return nil
}

func parseFloat64(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

// Helper function to determine asset type
func getAssetType(assetCode string) string {
	// Clean asset code first
	cleanCode := strings.TrimRight(assetCode, "\x00")
	if len(cleanCode) > 4 {
		return "credit_alphanum12"
	}
	return "credit_alphanum4"
}
