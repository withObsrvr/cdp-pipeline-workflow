package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"bytes"

	"github.com/guregu/null"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// ClaimableBalanceEvent represents a processed claimable balance event
type ClaimableBalanceEvent struct {
	Type               string      `json:"type"`
	BalanceID          string      `json:"balance_id"`
	Claimants          []Claimant  `json:"claimants"`
	AssetCode          string      `json:"asset_code"`
	AssetIssuer        string      `json:"asset_issuer"`
	AssetType          string      `json:"asset_type"`
	AssetID            int64       `json:"asset_id"`
	AssetAmount        float64     `json:"asset_amount"`
	Sponsor            null.String `json:"sponsor"`
	Flags              uint32      `json:"flags"`
	LastModifiedLedger uint32      `json:"last_modified_ledger"`
	LedgerSequence     uint32      `json:"ledger_sequence"`
	Deleted            bool        `json:"deleted"`
	ClosedAt           time.Time   `json:"closed_at"`
	TxHash             string      `json:"tx_hash"`
}

type Claimant struct {
	Destination string             `json:"destination"`
	Predicate   xdr.ClaimPredicate `json:"predicate"`
}

type ClaimableBalanceProcessor struct {
	networkPassphrase string
	processors        []Processor
	mu                sync.RWMutex
	stats             struct {
		ProcessedEvents uint64
		CreatedEvents   uint64
		UpdatedEvents   uint64
		DeletedEvents   uint64
		LastEventTime   time.Time
	}
}

func NewClaimableBalanceProcessor(config map[string]interface{}) (*ClaimableBalanceProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	return &ClaimableBalanceProcessor{
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *ClaimableBalanceProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *ClaimableBalanceProcessor) Process(ctx context.Context, msg Message) error {
	lcm, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	ledgerHeader := lcm.LedgerHeaderHistoryEntry()
	closeTime, err := TimePointToUTCTimeStamp(ledgerHeader.Header.ScpValue.CloseTime)
	if err != nil {
		return fmt.Errorf("error getting close time: %w", err)
	}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, lcm)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %v", err)
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

		// Get the transaction metadata
		if v1Meta, ok := tx.UnsafeMeta.GetV1(); ok {
			// Process each operation's changes
			for i := range v1Meta.Operations {
				changes := v1Meta.Operations[i].Changes
				if changes == nil {
					continue
				}

				for _, change := range changes {
					// Only process created/updated/removed entries
					var entry *xdr.LedgerEntry
					var changeType xdr.LedgerEntryChangeType
					var deleted bool

					switch change.Type {
					case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
						entry = change.Created
						changeType = change.Type
					case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
						entry = change.Updated
						changeType = change.Type
					case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
						entry = change.State
						changeType = change.Type
						deleted = true
					default:
						continue
					}

					if entry == nil || entry.Data.Type != xdr.LedgerEntryTypeClaimableBalance {
						continue
					}

					event, err := p.transformClaimableBalance(*entry, changeType, deleted, closeTime, ledgerHeader)
					if err != nil {
						log.Printf("Error transforming claimable balance: %v", err)
						continue
					}

					// Update stats
					p.updateStats(changeType)

					// Forward to downstream processors
					if err := p.forwardToProcessors(ctx, event); err != nil {
						return fmt.Errorf("error forwarding event: %w", err)
					}
				}
			}
		}
	}

	return nil
}

func (p *ClaimableBalanceProcessor) transformClaimableBalance(
	entry xdr.LedgerEntry,
	changeType xdr.LedgerEntryChangeType,
	deleted bool,
	closeTime time.Time,
	header xdr.LedgerHeaderHistoryEntry,
) (*ClaimableBalanceEvent, error) {
	balance, ok := entry.Data.GetClaimableBalance()
	if !ok {
		return nil, fmt.Errorf("invalid claimable balance data")
	}

	balanceID, err := xdr.MarshalHex(balance.BalanceId)
	if err != nil {
		return nil, fmt.Errorf("invalid balance ID: %w", err)
	}

	// Convert asset to our format
	assetCode, assetIssuer, assetType := "", "", "native"
	if balance.Asset.Type != xdr.AssetTypeAssetTypeNative {
		var err error
		assetCode, assetIssuer, assetType, err = getAssetDetails(balance.Asset)
		if err != nil {
			return nil, fmt.Errorf("error getting asset details: %w", err)
		}
	}

	// Convert claimants
	claimants := make([]Claimant, len(balance.Claimants))
	for i, c := range balance.Claimants {
		claimants[i] = Claimant{
			Destination: c.MustV0().Destination.Address(),
			Predicate:   c.MustV0().Predicate,
		}
	}

	// Fix the sponsor field handling
	var sponsorStr null.String
	if entry.Ext.V == 1 && entry.Ext.V1.SponsoringId != nil {
		sponsorStr = null.StringFrom(entry.Ext.V1.SponsoringId.Address())
	}

	event := &ClaimableBalanceEvent{
		Type:               getEventType(changeType),
		BalanceID:          balanceID,
		Claimants:          claimants,
		AssetCode:          assetCode,
		AssetIssuer:        assetIssuer,
		AssetType:          assetType,
		AssetID:            computeAssetID(assetCode, assetIssuer, assetType),
		AssetAmount:        float64(balance.Amount) / 10000000.0, // Convert from stroop to XLM
		Sponsor:            sponsorStr,
		Flags:              uint32(balance.Flags()),
		LastModifiedLedger: uint32(entry.LastModifiedLedgerSeq),
		LedgerSequence:     uint32(header.Header.LedgerSeq),
		Deleted:            deleted,
		ClosedAt:           closeTime,
	}

	return event, nil
}

func (p *ClaimableBalanceProcessor) updateStats(changeType xdr.LedgerEntryChangeType) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats.ProcessedEvents++
	p.stats.LastEventTime = time.Now()

	switch changeType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		p.stats.CreatedEvents++
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		p.stats.UpdatedEvents++
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		p.stats.DeletedEvents++
	}
}

func (p *ClaimableBalanceProcessor) forwardToProcessors(ctx context.Context, event *ClaimableBalanceEvent) error {
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling event: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	return nil
}

func (p *ClaimableBalanceProcessor) GetStats() struct {
	ProcessedEvents uint64
	CreatedEvents   uint64
	UpdatedEvents   uint64
	DeletedEvents   uint64
	LastEventTime   time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

// Helper functions
func getEventType(changeType xdr.LedgerEntryChangeType) string {
	switch changeType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		return "create_claimable_balance"
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		return "update_claimable_balance"
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return "remove_claimable_balance"
	default:
		return "unknown"
	}
}

func getAssetDetails(asset xdr.Asset) (code, issuer, assetType string, err error) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		alphaNum4 := asset.MustAlphaNum4()
		code = string(bytes.TrimRight(alphaNum4.AssetCode[:], "\x00"))
		issuer = alphaNum4.Issuer.Address()
		assetType = "credit_alphanum4"
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		alphaNum12 := asset.MustAlphaNum12()
		code = string(bytes.TrimRight(alphaNum12.AssetCode[:], "\x00"))
		issuer = alphaNum12.Issuer.Address()
		assetType = "credit_alphanum12"
	default:
		err = fmt.Errorf("unknown asset type: %v", asset.Type)
	}
	return code, issuer, assetType, err
}
