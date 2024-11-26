package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/stellar/go/amount"
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
}

// MarketMetricsProcessor processes ledger data into market metrics
type MarketMetricsProcessor struct {
	processors        []Processor
	networkPassphrase string
	metrics           map[string]*MarketMetrics // key is pair_name
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
			trade, err := p.extractTradeFromOperation(op, tx, int64(ledgerCloseMeta.LedgerSequence()))
			if err != nil {
				log.Printf("Error extracting trade: %v", err)
				continue
			}
			if trade != nil {
				metrics := p.calculateMetrics(trade, op.Body.Type)
				if metrics != nil {
					if err := p.processMetrics(ctx, metrics); err != nil {
						log.Printf("Error processing metrics for pair %s: %v",
							metrics.PairName, err)
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

func (p *MarketMetricsProcessor) calculateMetrics(trade *AppTrade, opType xdr.OperationType) *MarketMetrics {
	if trade.SellAsset == "" || trade.BuyAsset == "" {
		log.Printf("Warning: Invalid trade assets - Sell: %s, Buy: %s",
			trade.SellAsset, trade.BuyAsset)
		return nil
	}

	pairName := fmt.Sprintf("%s/%s", trade.SellAsset, trade.BuyAsset)
	metrics, exists := p.metrics[pairName]
	if !exists {
		metrics = &MarketMetrics{
			PairName:   pairName,
			BaseAsset:  trade.SellAsset,
			QuoteAsset: trade.BuyAsset,
			Type:       "trade", // Set default type
		}
		p.metrics[pairName] = metrics
	}

	// Convert Unix timestamp to time.Time
	timestampInt, err := strconv.ParseInt(trade.Timestamp, 10, 64)
	if err != nil {
		log.Printf("Error parsing timestamp: %v", err)
		return nil
	}

	// Create UTC timestamp
	tradeTime := time.Unix(timestampInt, 0).UTC()

	price := trade.Price
	baseAmount, _ := strconv.ParseFloat(trade.SellAmount, 64)
	counterAmount := baseAmount * price

	// Update metrics
	metrics.TradeCount24h++
	metrics.BaseVolume24h += baseAmount
	metrics.CounterVolume24h += counterAmount

	if metrics.Open24h == 0 {
		metrics.Open24h = price
	}

	if price > metrics.High24h {
		metrics.High24h = price
	}

	if price < metrics.Low24h || metrics.Low24h == 0 {
		metrics.Low24h = price
	}

	metrics.Close24h = price
	metrics.LastTradeAt = tradeTime
	metrics.GeneratedAt = time.Now()
	metrics.Change24h = ((metrics.Close24h - metrics.Open24h) / metrics.Open24h) * 100

	// Calculate spread and mid point
	metrics.Spread = metrics.High24h - metrics.Low24h
	if metrics.High24h > 0 && metrics.Low24h > 0 {
		metrics.SpreadMidPoint = (metrics.High24h + metrics.Low24h) / 2
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

	// Validate required fields
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
	trade := &AppTrade{
		Timestamp: fmt.Sprintf("%d", closeTime),
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

	// Extract asset details
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
