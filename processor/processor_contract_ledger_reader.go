package processor

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// Data type definitions
type ContractAssetData struct {
	ContractID     string    `json:"contract_id"`
	AssetType      string    `json:"asset_type"`
	AssetCode      string    `json:"asset_code"`
	AssetIssuer    string    `json:"asset_issuer"`
	OperationType  string    `json:"operation_type"`
	LastModified   uint32    `json:"last_modified"`
	LedgerSequence uint32    `json:"ledger_sequence"`
	Timestamp      time.Time `json:"timestamp"`
}

type ContractBalanceData struct {
	ContractID     string    `json:"contract_id"`
	BalanceHolder  string    `json:"balance_holder"`
	Balance        string    `json:"balance"`
	OperationType  string    `json:"operation_type"`
	LastModified   uint32    `json:"last_modified"`
	LedgerSequence uint32    `json:"ledger_sequence"`
	Timestamp      time.Time `json:"timestamp"`
}

type ContractLedgerReader struct {
	processors        []Processor
	networkPassphrase string
	mu                sync.RWMutex
	stats             struct {
		ProcessedLedgers uint32
		ProcessedChanges uint64
		AssetsFound      uint64
		BalancesFound    uint64
		LastLedger       uint32
		LastUpdateTime   time.Time
	}
}

// Helper functions for asset extraction
func AssetFromContractData(ledgerEntry xdr.LedgerEntry, passphrase string) *xdr.Asset {
	contractData, ok := ledgerEntry.Data.GetContractData()
	if !ok {
		return nil
	}

	if contractData.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
		return nil
	}

	// Basic implementation - you'll want to expand this based on your needs
	if contractData.Contract.ContractId != nil {
		// Process asset data
		// This is a simplified version - you'll want to implement the full logic
		return nil
	}

	return nil
}

// Helper functions for balance extraction
func ContractBalanceFromContractData(ledgerEntry xdr.LedgerEntry, passphrase string) ([32]byte, *big.Int, bool) {
	contractData, ok := ledgerEntry.Data.GetContractData()
	if !ok {
		return [32]byte{}, nil, false
	}

	if contractData.Contract.ContractId == nil {
		return [32]byte{}, nil, false
	}

	// Basic implementation - you'll want to expand this based on your needs
	holder := [32]byte{}
	balance := big.NewInt(0)
	return holder, balance, true
}

func NewContractLedgerReader(config map[string]interface{}) (*ContractLedgerReader, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	return &ContractLedgerReader{
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *ContractLedgerReader) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *ContractLedgerReader) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	sequence := ledgerCloseMeta.LedgerSequence()
	log.Printf("Processing ledger %d for contract data", sequence)

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

		// Process each operation's changes
		if tx.UnsafeMeta.V1 == nil {
			continue
		}

		for i := range tx.UnsafeMeta.V1.Operations {
			changes := tx.UnsafeMeta.V1.Operations[i].Changes
			for _, change := range changes {
				// First, check if this change involves contract data
				var entry *xdr.LedgerEntry
				var changeType string

				switch change.Type {
				case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
					entry = change.Created
					changeType = "created"
				case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
					entry = change.Updated
					changeType = "updated"
				case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
					entry = change.State
					changeType = "removed"
				default:
					continue
				}

				if entry == nil {
					continue
				}

				// Now examine the contract data itself
				contractData, ok := entry.Data.GetContractData()
				if !ok {
					continue
				}

				// Process the contract data
				if err := p.processContractData(ctx, &contractData, entry, changeType, ledgerCloseMeta.LedgerHeaderHistoryEntry()); err != nil {
					log.Printf("Error processing contract data: %v", err)
					continue
				}
			}
		}
	}

	p.mu.Lock()
	p.stats.ProcessedLedgers++
	p.stats.LastLedger = sequence
	p.stats.LastUpdateTime = time.Now()
	p.mu.Unlock()

	return nil
}

func (p *ContractLedgerReader) processContractData(
	ctx context.Context,
	contractData *xdr.ContractDataEntry,
	entry *xdr.LedgerEntry,
	changeType string,
	header xdr.LedgerHeaderHistoryEntry,
) error {
	if contractData.Contract.ContractId == nil {
		return fmt.Errorf("contract ID is nil")
	}

	contractID, err := strkey.Encode(strkey.VersionByteContract, contractData.Contract.ContractId[:])
	if err != nil {
		return fmt.Errorf("error encoding contract ID: %w", err)
	}

	log.Printf("Processing contract data for contract ID: %s (change type: %s)", contractID, changeType)

	// Extract asset data if present
	if asset := AssetFromContractData(*entry, p.networkPassphrase); asset != nil {
		p.mu.Lock()
		p.stats.AssetsFound++
		p.mu.Unlock()

		log.Printf("Found asset contract: %s (type: %s, code: %s, issuer: %s)",
			contractID,
			asset.Type.String(),
			asset.GetCode(),
			asset.GetIssuer())

		assetData := ContractAssetData{
			ContractID:     contractID,
			AssetType:      asset.Type.String(),
			AssetCode:      asset.GetCode(),
			AssetIssuer:    asset.GetIssuer(),
			OperationType:  changeType,
			LastModified:   uint32(entry.LastModifiedLedgerSeq),
			LedgerSequence: uint32(header.Header.LedgerSeq),
			Timestamp:      time.Unix(int64(header.Header.ScpValue.CloseTime), 0),
		}

		return p.forwardToProcessors(ctx, assetData)
	}

	// Extract balance data if present
	if holder, balance, ok := ContractBalanceFromContractData(*entry, p.networkPassphrase); ok {
		p.mu.Lock()
		p.stats.BalancesFound++
		p.mu.Unlock()

		holderID, err := strkey.Encode(strkey.VersionByteContract, holder[:])
		if err != nil {
			return fmt.Errorf("error encoding holder ID: %w", err)
		}

		log.Printf("Found balance contract: %s (holder: %s, balance: %s)",
			contractID,
			holderID,
			balance.String())

		balanceData := ContractBalanceData{
			ContractID:     contractID,
			BalanceHolder:  holderID,
			Balance:        balance.String(),
			OperationType:  changeType,
			LastModified:   uint32(entry.LastModifiedLedgerSeq),
			LedgerSequence: uint32(header.Header.LedgerSeq),
			Timestamp:      time.Unix(int64(header.Header.ScpValue.CloseTime), 0),
		}

		return p.forwardToProcessors(ctx, balanceData)
	}

	log.Printf("Contract %s did not match asset or balance patterns", contractID)
	return nil
}

func (p *ContractLedgerReader) forwardToProcessors(ctx context.Context, data interface{}) error {
	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: data}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	return nil
}

func (p *ContractLedgerReader) GetStats() struct {
	ProcessedLedgers uint32
	ProcessedChanges uint64
	AssetsFound      uint64
	BalancesFound    uint64
	LastLedger       uint32
	LastUpdateTime   time.Time
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}
