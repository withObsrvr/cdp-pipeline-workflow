package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

type AppTrade struct {
	Timestamp        string       `json:"timestamp"`
	SellerId         string       `json:"seller_id"`
	BuyerId          string       `json:"buyer_id"`
	SellAsset        string       `json:"sell_asset"`
	BuyAsset         string       `json:"buy_asset"`
	SellAmount       string       `json:"sell_amount"`
	BuyAmount        string       `json:"buy_amount"`
	Price            float64      `json:"price"`
	Type             string       `json:"type"`
	OrderbookID      string       `json:"orderbook_id"`
	LiquidityPool    string       `json:"liquidity_pool,omitempty"`
	SellAssetDetails AssetDetails `json:"sell_asset_details"`
	BuyAssetDetails  AssetDetails `json:"buy_asset_details"`
}

type TransformToAppTrade struct {
	networkPassphrase string
	processors        []Processor
}

func NewTransformToAppTrade(config map[string]interface{}) (*TransformToAppTrade, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	return &TransformToAppTrade{
		networkPassphrase: networkPassphrase,
	}, nil
}

func (t *TransformToAppTrade) Subscribe(processor Processor) {
	t.processors = append(t.processors, processor)
}

func (t *TransformToAppTrade) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in TransformToAppTrade")
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(t.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("failed to create reader for ledger %v: %w", ledgerCloseMeta.LedgerSequence(), err)
	}

	closeTime := uint32(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)

	var transaction ingest.LedgerTransaction
	for {
		transaction, err = ledgerTxReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		for _, op := range transaction.Envelope.Operations() {
			switch op.Body.Type {
			case xdr.OperationTypeManageBuyOffer, xdr.OperationTypeManageSellOffer:
				trade, err := t.extractTradeFromOperation(op, transaction, closeTime)
				if err != nil {
					log.Printf("Error extracting trade: %v", err)
					continue
				}

				if trade != nil {
					jsonBytes, err := json.Marshal(trade)
					if err != nil {
						return fmt.Errorf("error marshaling trade: %w", err)
					}

					for _, processor := range t.processors {
						if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
							return fmt.Errorf("error in processor chain: %w", err)
						}
					}
					log.Printf("Successfully forwarded trade: %+v", trade)
				}
			}
		}
	}

	return nil
}

func (t *TransformToAppTrade) extractTradeFromOperation(op xdr.Operation, tx ingest.LedgerTransaction, closeTime uint32) (*AppTrade, error) {
	trade := &AppTrade{
		Timestamp: fmt.Sprintf("%d", closeTime),
		Type:      "trade",
		SellerId:  tx.Envelope.SourceAccount().ToAccountId().Address(),
	}

	switch op.Body.Type {
	case xdr.OperationTypeManageBuyOffer:
		offer := op.Body.MustManageBuyOfferOp()
		trade.SellAsset = offer.Selling.StringCanonical()
		trade.BuyAsset = offer.Buying.StringCanonical()
		trade.SellAmount = amount.String(offer.BuyAmount)
		trade.BuyAmount = amount.String(offer.BuyAmount)
		trade.Price = float64(offer.Price.N) / float64(offer.Price.D)
		trade.OrderbookID = fmt.Sprintf("%d", offer.OfferId)

	case xdr.OperationTypeManageSellOffer:
		offer := op.Body.MustManageSellOfferOp()
		trade.SellAsset = offer.Selling.StringCanonical()
		trade.BuyAsset = offer.Buying.StringCanonical()
		trade.SellAmount = amount.String(offer.Amount)
		trade.BuyAmount = amount.String(offer.Amount)
		trade.Price = float64(offer.Price.N) / float64(offer.Price.D)
		trade.OrderbookID = fmt.Sprintf("%d", offer.OfferId)

	default:
		return nil, nil
	}

	// Validate trade data
	if trade.SellAsset == "" || trade.BuyAsset == "" ||
		trade.SellAmount == "" || trade.BuyAmount == "" {
		return nil, nil // Skip invalid trades without error
	}

	// Parse asset details
	sellAssetDetails, err := t.parseAssetDetails(trade.SellAsset)
	if err != nil {
		return nil, fmt.Errorf("error parsing sell asset: %w", err)
	}
	trade.SellAssetDetails = sellAssetDetails

	buyAssetDetails, err := t.parseAssetDetails(trade.BuyAsset)
	if err != nil {
		return nil, fmt.Errorf("error parsing buy asset: %w", err)
	}
	trade.BuyAssetDetails = buyAssetDetails

	return trade, nil
}

func (t *TransformToAppTrade) parseAssetDetails(assetStr string) (AssetDetails, error) {
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
