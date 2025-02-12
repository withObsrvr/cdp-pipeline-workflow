package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest/ledger"
	"github.com/stellar/go/xdr"

	"github.com/stellar/go/ingest"
)

// AnalyticsPeriod represents different time periods for analytics
type AnalyticsPeriod string

const (
	Hour  AnalyticsPeriod = "1h"
	Day   AnalyticsPeriod = "24h"
	Week  AnalyticsPeriod = "7d"
	Month AnalyticsPeriod = "30d"
)

// TradeWindowEntry is used to store individual trade contributions within a sliding window.
type TradeWindowEntry struct {
	Timestamp     time.Time
	Price         float64
	BaseVolume    float64
	CounterVolume float64
}

// MarketMetrics represents market analysis data for a trading pair
type MarketMetrics struct {
	PairName         string    `json:"pair_name"`
	BaseAsset        string    `json:"base_asset"`
	QuoteAsset       string    `json:"quote_asset"`
	TradeCount24h    int64     `json:"trade_count_24h"`
	BaseVolume24h    float64   `json:"base_volume_24h"`
	CounterVolume24h float64   `json:"counter_volume_24h"`
	Open24h          float64   `json:"open_24h"`
	High24h          float64   `json:"high_24h"`
	Low24h           float64   `json:"low_24h"`
	Close24h         float64   `json:"close_24h"`
	Change24h        float64   `json:"change_24h"`
	LastTradeAt      time.Time `json:"last_trade_at"`
	GeneratedAt      time.Time `json:"generated_at"`
	Type             string    `json:"type"`
	BidVolume        float64   `json:"bid_volume"`
	AskVolume        float64   `json:"ask_volume"`
	Spread           float64   `json:"spread"`
	SpreadMidPoint   float64   `json:"spread_midpoint"`

	// TradesWindow holds individual trade entries in the last 24 hours.
	TradesWindow []TradeWindowEntry `json:"-"`
}

// MarketMetricsProcessor processes ledger data into market metrics
type MarketMetricsProcessor struct {
	processors        []Processor
	networkPassphrase string
	metrics           map[string]*MarketMetrics // key is pair_name
	metricsMu         sync.Mutex                // protects metrics
}

// NewMarketMetricsProcessor creates a new market metrics processor
func NewMarketMetricsProcessor(config map[string]interface{}) (*MarketMetricsProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration: missing 'network_passphrase'")
	}
	return &MarketMetricsProcessor{
		networkPassphrase: networkPassphrase,
		metrics:           make(map[string]*MarketMetrics),
		processors:        make([]Processor, 0),
	}, nil
}

func (p *MarketMetricsProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		for _, op := range tx.Envelope.Operations() {
			// Get the ledger close time from the ledger meta
			ledgerCloseTime := ledger.ClosedAt(ledgerCloseMeta)
			trade, err := p.extractTradeFromOperation(op, tx, ledgerCloseTime.Unix())
			if err != nil {
				log.Printf("Error extracting trade: %v", err)
				continue
			}
			if trade != nil {
				metrics := p.calculateMetrics(trade, op.Body.Type)
				if metrics != nil {
					if err := p.processMetrics(ctx, metrics); err != nil {
						log.Printf("Error processing metrics for pair %s: %v", metrics.PairName, err)
					}
				}
			}
		}
	}

	return nil
}

// Subscribe sets up the next processor in the chain
func (p *MarketMetricsProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

// calculateMetrics aggregates trade data using a sliding window (last 24 hours).
// It purges old data, recalculates aggregate metrics, and logs processing time for instrumentation.
func (p *MarketMetricsProcessor) calculateMetrics(trade *AppTrade, opType xdr.OperationType) *MarketMetrics {
	// Begin instrumentation time.
	startTime := time.Now()

	if trade.SellAsset == "" || trade.BuyAsset == "" {
		log.Printf("Warning: Invalid trade assets - Sell: %s, Buy: %s", trade.SellAsset, trade.BuyAsset)
		return nil
	}

	pairName := fmt.Sprintf("%s/%s", trade.SellAsset, trade.BuyAsset)
	p.metricsMu.Lock()
	defer p.metricsMu.Unlock()
	metrics, exists := p.metrics[pairName]
	if !exists {
		metrics = &MarketMetrics{
			PairName:     pairName,
			BaseAsset:    trade.SellAsset,
			QuoteAsset:   trade.BuyAsset,
			Type:         "trade",
			TradesWindow: make([]TradeWindowEntry, 0),
		}
		p.metrics[pairName] = metrics
	}

	// Parse trade timestamp (stored as RFC3339).
	tradeTime, err := time.Parse(time.RFC3339, trade.Timestamp)
	if err != nil {
		log.Printf("Error parsing trade timestamp: %v", err)
		return nil
	}

	// Parse volume and price.
	baseAmount, _ := strconv.ParseFloat(trade.SellAmount, 64)
	counterAmount := baseAmount * trade.Price

	// Append new trade into the sliding window.
	newEntry := TradeWindowEntry{
		Timestamp:     tradeTime,
		Price:         trade.Price,
		BaseVolume:    baseAmount,
		CounterVolume: counterAmount,
	}
	metrics.TradesWindow = append(metrics.TradesWindow, newEntry)

	// Purge trades older than 24 hours relative to current tradeTime.
	windowDuration := 24 * time.Hour
	cutoff := tradeTime.Add(-windowDuration)
	var updatedWindow []TradeWindowEntry
	for _, entry := range metrics.TradesWindow {
		if entry.Timestamp.After(cutoff) {
			updatedWindow = append(updatedWindow, entry)
		}
	}
	metrics.TradesWindow = updatedWindow

	// Recompute metrics based on the sliding window.
	var open, close, high, low, sumBase, sumCounter float64
	var count int64
	if len(metrics.TradesWindow) > 0 {
		count = int64(len(metrics.TradesWindow))
		open = metrics.TradesWindow[0].Price                            // oldest trade
		close = metrics.TradesWindow[len(metrics.TradesWindow)-1].Price // newest trade
		high = open
		low = open
		for _, entry := range metrics.TradesWindow {
			if entry.Price > high {
				high = entry.Price
			}
			if entry.Price < low {
				low = entry.Price
			}
			sumBase += entry.BaseVolume
			sumCounter += entry.CounterVolume
		}
	}

	// Update aggregated metrics.
	metrics.TradeCount24h = count
	metrics.Open24h = open
	metrics.Close24h = close
	metrics.High24h = high
	metrics.Low24h = low
	metrics.BaseVolume24h = sumBase
	metrics.CounterVolume24h = sumCounter
	if open > 0 {
		metrics.Change24h = ((close - open) / open) * 100
	} else {
		metrics.Change24h = 0
	}
	metrics.LastTradeAt = tradeTime
	metrics.GeneratedAt = time.Now()
	metrics.Spread = high - low
	if high > 0 && low > 0 {
		metrics.SpreadMidPoint = (high + low) / 2
	}

	// Instrumentation: measure processing duration.
	elapsed := time.Since(startTime)
	// Log if processing took longer than 10 milliseconds (adjust threshold as needed).
	if elapsed > 10*time.Millisecond {
		log.Printf("calculateMetrics for pair %s took %s", pairName, elapsed)
	}

	return metrics
}

func (p *MarketMetricsProcessor) processMetrics(ctx context.Context, metrics *MarketMetrics) error {
	tradePair := fmt.Sprintf("%s/%s", metrics.BaseAsset, metrics.QuoteAsset)

	analytics := MarketAnalytics{
		TradePair:      tradePair,
		Period:         string(Day),
		BaseAsset:      metrics.BaseAsset,
		CounterAsset:   metrics.QuoteAsset,
		OpenPrice:      metrics.Open24h,
		ClosePrice:     metrics.Close24h,
		HighPrice:      metrics.High24h,
		LowPrice:       metrics.Low24h,
		Volume:         metrics.BaseVolume24h,
		TradeCount:     metrics.TradeCount24h,
		PriceChange:    metrics.Change24h,
		Timestamp:      metrics.LastTradeAt,
		BidVolume:      metrics.BidVolume,
		AskVolume:      metrics.AskVolume,
		Spread:         metrics.Spread,
		SpreadMidPoint: metrics.SpreadMidPoint,
		Type:           metrics.Type,
	}

	// Validate required fields.
	if analytics.TradePair == "" || analytics.TradePair == "/" {
		return fmt.Errorf("missing trade_pair in data")
	}

	jsonBytes, err := json.Marshal(analytics)
	if err != nil {
		return fmt.Errorf("error marshaling analytics: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	return nil
}

func (p *MarketMetricsProcessor) extractTradeFromOperation(op xdr.Operation, tx ingest.LedgerTransaction, closeTime int64) (*AppTrade, error) {
	// Create a trade using the new ledger close time. We convert the Unix timestamp
	// into an RFC3339-formatted string so that downstream processors can parse it.
	trade := &AppTrade{
		Timestamp: time.Unix(closeTime, 0).UTC().Format(time.RFC3339),
		Type:      "trade",
		SellerId:  tx.Envelope.SourceAccount().ToAccountId().Address(),
	}

	switch op.Body.Type {
	case xdr.OperationTypeManageBuyOffer:
		offer := op.Body.ManageBuyOfferOp
		trade.SellAsset = offer.Selling.StringCanonical()
		trade.BuyAsset = offer.Buying.StringCanonical()
		trade.SellAmount = amount.String(offer.BuyAmount)
		trade.BuyAmount = amount.String(offer.BuyAmount)
		trade.Price = float64(offer.Price.N) / float64(offer.Price.D)
		trade.OrderbookID = fmt.Sprintf("%d", offer.OfferId)
	case xdr.OperationTypeManageSellOffer:
		offer := op.Body.ManageSellOfferOp
		trade.SellAsset = offer.Selling.StringCanonical()
		trade.BuyAsset = offer.Buying.StringCanonical()
		trade.SellAmount = amount.String(offer.Amount)
		trade.BuyAmount = amount.String(offer.Amount)
		trade.Price = float64(offer.Price.N) / float64(offer.Price.D)
		trade.OrderbookID = fmt.Sprintf("%d", offer.OfferId)
	default:
		return nil, nil
	}

	// Extract asset details (if needed).
	sellAssetDetails, err := parseAssetDetails(trade.SellAsset)
	if err != nil {
		return nil, fmt.Errorf("error parsing sell asset: %w", err)
	}
	trade.SellAssetDetails = sellAssetDetails

	buyAssetDetails, err := parseAssetDetails(trade.BuyAsset)
	if err != nil {
		return nil, fmt.Errorf("error parsing buy asset: %w", err)
	}
	trade.BuyAssetDetails = buyAssetDetails

	return trade, nil
}

// MarketAnalytics represents aggregated market data for a trading pair
type MarketAnalytics struct {
	TradePair      string    `json:"trade_pair"`
	Period         string    `json:"period"`
	BaseAsset      string    `json:"base_asset"`
	CounterAsset   string    `json:"counter_asset"`
	OpenPrice      float64   `json:"open_price"`
	ClosePrice     float64   `json:"close_price"`
	HighPrice      float64   `json:"high_price"`
	LowPrice       float64   `json:"low_price"`
	Volume         float64   `json:"volume"`
	TradeCount     int64     `json:"trade_count"`
	PriceChange    float64   `json:"price_change"`
	Timestamp      time.Time `json:"timestamp"`
	BidVolume      float64   `json:"bid_volume"`
	AskVolume      float64   `json:"ask_volume"`
	Spread         float64   `json:"spread"`
	SpreadMidPoint float64   `json:"spread_mid_point"`
	Type           string    `json:"type"`
}
