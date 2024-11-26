package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// TickerOrderbook represents the orderbook data for a market
type TickerOrderbook struct {
	BaseAsset    string    `json:"base_asset"`
	QuoteAsset   string    `json:"quote_asset"`
	BidCount     int       `json:"bid_count"`
	BidVolume    float64   `json:"bid_volume"`
	BidMax       float64   `json:"bid_max"`
	AskCount     int       `json:"ask_count"`
	AskVolume    float64   `json:"ask_volume"`
	AskMin       float64   `json:"ask_min"`
	Spread       float64   `json:"spread"`
	SpreadMid    float64   `json:"spread_mid"`
	LastModified time.Time `json:"last_modified"`
}

type TransformToTickerOrderbookProcessor struct {
	processors        []Processor
	networkPassphrase string
}

func NewTransformToTickerOrderbookProcessor(config map[string]interface{}) (*TransformToTickerOrderbookProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration: missing 'network_passphrase'")
	}
	return &TransformToTickerOrderbookProcessor{
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *TransformToTickerOrderbookProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *TransformToTickerOrderbookProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		payloadBytes, ok := msg.Payload.([]byte)
		if !ok {
			return fmt.Errorf("expected xdr.LedgerCloseMeta or []byte payload, got %T", msg.Payload)
		}
		return p.processTrade(ctx, payloadBytes)
	}

	closeTime := ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %v", err)
	}
	defer txReader.Close()

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %v", err)
		}

		for _, op := range tx.Envelope.Operations() {
			trade, err := p.extractTradeFromOperation(op, tx, int64(closeTime))
			if err != nil {
				return fmt.Errorf("error extracting trade: %v", err)
			}
			if trade != nil {
				if err := p.processTrade(ctx, trade); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (p *TransformToTickerOrderbookProcessor) processTrade(ctx context.Context, payload interface{}) error {
	var trade AppTrade
	switch v := payload.(type) {
	case []byte:
		if err := json.Unmarshal(v, &trade); err != nil {
			return fmt.Errorf("error unmarshaling trade: %w", err)
		}
	case *AppTrade:
		trade = *v
	default:
		return fmt.Errorf("unexpected payload type: %T", payload)
	}

	// Create orderbook entry from trade
	orderbook := &TickerOrderbook{
		BaseAsset:    fmt.Sprintf("%s:%s", trade.SellAssetDetails.Code, trade.SellAssetDetails.Issuer),
		QuoteAsset:   fmt.Sprintf("%s:%s", trade.BuyAssetDetails.Code, trade.BuyAssetDetails.Issuer),
		LastModified: time.Now(),
	}

	// Update orderbook stats based on trade
	if trade.Price > 0 {
		orderbook.BidMax = trade.Price
		orderbook.AskMin = trade.Price
		orderbook.Spread = 0
		orderbook.SpreadMid = trade.Price
	}

	// Forward orderbook data to next processors
	jsonBytes, err := json.Marshal(orderbook)
	if err != nil {
		return fmt.Errorf("error marshaling orderbook: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

func (p *TransformToTickerOrderbookProcessor) extractTradeFromOperation(op xdr.Operation, tx ingest.LedgerTransaction, closeTime int64) (*AppTrade, error) {
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

	// Only process valid trades
	if trade.SellAsset == "" || trade.BuyAsset == "" ||
		trade.SellAmount == "" || trade.BuyAmount == "" {
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

func parseAssetDetails(assetStr string) (AssetDetails, error) {
	parts := strings.Split(assetStr, ":")

	details := AssetDetails{}

	if len(parts) == 1 && parts[0] == "native" {
		details.Code = "XLM"
		details.Type = "native"
	} else if len(parts) == 2 {
		details.Code = parts[0]
		details.Issuer = parts[1]
		details.Type = "credit_alphanum4"
		if len(parts[0]) > 4 {
			details.Type = "credit_alphanum12"
		}
	} else {
		return details, fmt.Errorf("invalid asset format: %s", assetStr)
	}

	return details, nil
}
