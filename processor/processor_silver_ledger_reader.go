package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// SilverLedgerReaderProcessor extracts all 19 Silver table data types from LedgerCloseMeta
// This processor is designed to work with the SilverCopierConsumer which writes to DuckLake
type SilverLedgerReaderProcessor struct {
	networkPassphrase string
	subscribers       []Processor
}

// NewSilverLedgerReaderProcessor creates a new Silver ledger reader processor
func NewSilverLedgerReaderProcessor(config map[string]interface{}) *SilverLedgerReaderProcessor {
	networkPassphrase := "Test SDF Network ; September 2015"
	if np, ok := config["network_passphrase"].(string); ok {
		networkPassphrase = np
	}

	return &SilverLedgerReaderProcessor{
		networkPassphrase: networkPassphrase,
		subscribers:       make([]Processor, 0),
	}
}

// Subscribe adds a subscriber to receive processed messages
func (p *SilverLedgerReaderProcessor) Subscribe(processor Processor) {
	p.subscribers = append(p.subscribers, processor)
}

// Process extracts all Silver data types from LedgerCloseMeta and emits to subscribers
func (p *SilverLedgerReaderProcessor) Process(ctx context.Context, msg Message) error {
	// Expect LedgerCloseMeta payload
	lcm, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("SilverLedgerReader: expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	// Get ledger sequence and closed_at for all records
	ledgerSeq := p.getLedgerSequence(&lcm)
	closedAt := p.getClosedAt(&lcm)
	ledgerRange := (ledgerSeq / 10000) * 10000 // Partition key

	// Extract and emit each data type
	// 1. Ledgers
	ledgerData := p.extractLedgerData(&lcm, ledgerRange)
	if err := p.emit(ctx, "ledgers_row_v2", ledgerData); err != nil {
		return err
	}

	// 2. Transactions
	transactions := p.extractTransactions(&lcm, closedAt, ledgerRange)
	for _, tx := range transactions {
		if err := p.emit(ctx, "transactions_row_v2", tx); err != nil {
			return err
		}
	}

	// 3. Operations
	operations := p.extractOperations(&lcm, closedAt, ledgerRange)
	for _, op := range operations {
		if err := p.emit(ctx, "operations_row_v2", op); err != nil {
			return err
		}
	}

	// 4. Effects
	effects := p.extractEffects(&lcm, closedAt, ledgerRange)
	for _, effect := range effects {
		if err := p.emit(ctx, "effects_row_v1", effect); err != nil {
			return err
		}
	}

	// 5. Trades
	trades := p.extractTrades(&lcm, closedAt, ledgerRange)
	for _, trade := range trades {
		if err := p.emit(ctx, "trades_row_v1", trade); err != nil {
			return err
		}
	}

	// 6. Native Balances
	balances := p.extractNativeBalances(&lcm, ledgerSeq, ledgerRange)
	for _, balance := range balances {
		if err := p.emit(ctx, "native_balances_snapshot_v1", balance); err != nil {
			return err
		}
	}

	// 7. Accounts
	accounts := p.extractAccounts(&lcm, closedAt, ledgerRange)
	for _, account := range accounts {
		if err := p.emit(ctx, "accounts_snapshot_v1", account); err != nil {
			return err
		}
	}

	// 8. Trustlines
	trustlines := p.extractTrustlines(&lcm, ledgerSeq, ledgerRange)
	for _, trustline := range trustlines {
		if err := p.emit(ctx, "trustlines_snapshot_v1", trustline); err != nil {
			return err
		}
	}

	// 9. Offers
	offers := p.extractOffers(&lcm, closedAt, ledgerRange)
	for _, offer := range offers {
		if err := p.emit(ctx, "offers_snapshot_v1", offer); err != nil {
			return err
		}
	}

	// 10. Claimable Balances
	claimableBalances := p.extractClaimableBalances(&lcm, closedAt, ledgerRange)
	for _, cb := range claimableBalances {
		if err := p.emit(ctx, "claimable_balances_snapshot_v1", cb); err != nil {
			return err
		}
	}

	// 11. Liquidity Pools
	liquidityPools := p.extractLiquidityPools(&lcm, closedAt, ledgerRange)
	for _, lp := range liquidityPools {
		if err := p.emit(ctx, "liquidity_pools_snapshot_v1", lp); err != nil {
			return err
		}
	}

	// 12. Contract Events
	contractEvents := p.extractContractEvents(&lcm, closedAt, ledgerRange)
	for _, event := range contractEvents {
		if err := p.emit(ctx, "contract_events_stream_v1", event); err != nil {
			return err
		}
	}

	// 13. Contract Data
	contractData := p.extractContractData(&lcm, closedAt, ledgerRange)
	for _, data := range contractData {
		if err := p.emit(ctx, "contract_data_snapshot_v1", data); err != nil {
			return err
		}
	}

	// 14. Contract Code
	contractCode := p.extractContractCode(&lcm, closedAt, ledgerRange)
	for _, code := range contractCode {
		if err := p.emit(ctx, "contract_code_snapshot_v1", code); err != nil {
			return err
		}
	}

	// 15. Config Settings
	configSettings := p.extractConfigSettings(&lcm, closedAt, ledgerRange)
	for _, config := range configSettings {
		if err := p.emit(ctx, "config_settings_snapshot_v1", config); err != nil {
			return err
		}
	}

	// 16. TTL
	ttl := p.extractTTL(&lcm, closedAt, ledgerRange)
	for _, t := range ttl {
		if err := p.emit(ctx, "ttl_snapshot_v1", t); err != nil {
			return err
		}
	}

	// 17. Evicted Keys
	evictedKeys := p.extractEvictedKeys(&lcm, closedAt, ledgerRange)
	for _, key := range evictedKeys {
		if err := p.emit(ctx, "evicted_keys_state_v1", key); err != nil {
			return err
		}
	}

	// 18. Restored Keys
	restoredKeys := p.extractRestoredKeys(&lcm, closedAt, ledgerRange)
	for _, key := range restoredKeys {
		if err := p.emit(ctx, "restored_keys_state_v1", key); err != nil {
			return err
		}
	}

	// 19. Account Signers
	accountSigners := p.extractAccountSigners(&lcm, closedAt, ledgerRange)
	for _, signer := range accountSigners {
		if err := p.emit(ctx, "account_signers_snapshot_v1", signer); err != nil {
			return err
		}
	}

	return nil
}

// emit sends a message to all subscribers with table_type metadata
func (p *SilverLedgerReaderProcessor) emit(ctx context.Context, tableType string, record interface{}) error {
	msg := Message{
		Payload: record,
		Metadata: map[string]interface{}{
			"table_type": tableType,
		},
	}
	for _, sub := range p.subscribers {
		if err := sub.Process(ctx, msg); err != nil {
			return fmt.Errorf("failed to emit to subscriber for %s: %w", tableType, err)
		}
	}
	return nil
}

// Helper methods to get ledger info

func (p *SilverLedgerReaderProcessor) getLedgerSequence(lcm *xdr.LedgerCloseMeta) uint32 {
	switch lcm.V {
	case 0:
		return uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq)
	case 1:
		return uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq)
	case 2:
		return uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq)
	default:
		return 0
	}
}

func (p *SilverLedgerReaderProcessor) getClosedAt(lcm *xdr.LedgerCloseMeta) time.Time {
	switch lcm.V {
	case 0:
		return time.Unix(int64(lcm.MustV0().LedgerHeader.Header.ScpValue.CloseTime), 0)
	case 1:
		return time.Unix(int64(lcm.MustV1().LedgerHeader.Header.ScpValue.CloseTime), 0)
	case 2:
		return time.Unix(int64(lcm.MustV2().LedgerHeader.Header.ScpValue.CloseTime), 0)
	default:
		return time.Time{}
	}
}

func (p *SilverLedgerReaderProcessor) getLedgerHeader(lcm *xdr.LedgerCloseMeta) xdr.LedgerHeaderHistoryEntry {
	switch lcm.V {
	case 0:
		return lcm.MustV0().LedgerHeader
	case 1:
		return lcm.MustV1().LedgerHeader
	case 2:
		return lcm.MustV2().LedgerHeader
	default:
		return xdr.LedgerHeaderHistoryEntry{}
	}
}

// getTransactionReader creates an ingest.LedgerTransactionReader for the ledger
func (p *SilverLedgerReaderProcessor) getTransactionReader(lcm *xdr.LedgerCloseMeta) (*ingest.LedgerTransactionReader, error) {
	return ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
}

// Ensure SilverLedgerReaderProcessor implements Processor interface
var _ Processor = (*SilverLedgerReaderProcessor)(nil)
