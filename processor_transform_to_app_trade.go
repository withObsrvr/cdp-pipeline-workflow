package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/xdr"
)

type AppTrade struct {
	Timestamp     string  `json:"timestamp"`
	SellerId      string  `json:"seller_id"`
	BuyerId       string  `json:"buyer_id"`
	SellAsset     string  `json:"sell_asset"`
	BuyAsset      string  `json:"buy_asset"`
	SellAmount    string  `json:"sell_amount"`
	BuyAmount     string  `json:"buy_amount"`
	Price         float64 `json:"price"`
	Type          string  `json:"type"`
	OrderbookID   string  `json:"orderbook_id"`
	LiquidityPool string  `json:"liquidity_pool,omitempty"`
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
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg)
	}

	closeTime := uint(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)

	for _, tx := range ledgerCloseMeta.V0.TxSet.Txs {
		for _, op := range tx.Operations() {
			switch op.Body.Type {
			case xdr.OperationTypeManageBuyOffer,
				xdr.OperationTypeManageSellOffer,
				xdr.OperationTypeCreatePassiveSellOffer:

				trade := AppTrade{
					Timestamp: fmt.Sprintf("%d", closeTime),
					Type:      "trade",
					SellerId:  tx.SourceAccount().ToAccountId().Address(),
				}

				// Process different offer types
				switch op.Body.Type {
				case xdr.OperationTypeManageBuyOffer:
					offer := op.Body.ManageBuyOfferOp
					trade.SellAsset = offer.Selling.StringCanonical()
					trade.BuyAsset = offer.Buying.StringCanonical()
					trade.SellAmount = amount.String(offer.BuyAmount)
					trade.Price = float64(offer.Price.N) / float64(offer.Price.D)
					trade.OrderbookID = fmt.Sprintf("%d", offer.OfferId)

				case xdr.OperationTypeManageSellOffer:
					offer := op.Body.ManageSellOfferOp
					trade.SellAsset = offer.Selling.StringCanonical()
					trade.BuyAsset = offer.Buying.StringCanonical()
					trade.SellAmount = amount.String(offer.Amount)
					trade.Price = float64(offer.Price.N) / float64(offer.Price.D)
					trade.OrderbookID = fmt.Sprintf("%d", offer.OfferId)
				}

				jsonBytes, err := json.Marshal(trade)
				if err != nil {
					return fmt.Errorf("error marshaling trade: %w", err)
				}

				// Process trade through pipeline
				for _, processor := range t.processors {
					if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
						return fmt.Errorf("error processing trade: %w", err)
					}
				}
			}
		}
	}

	return nil
}
