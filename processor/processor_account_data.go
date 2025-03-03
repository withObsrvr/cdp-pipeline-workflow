package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// AccountDataConfig holds the configuration for the account data processor
type AccountDataConfig struct {
	NetworkPassphrase string
	Processors        []Processor
}

// ProcessAccountData processes Stellar account data
type ProcessAccountDataFull struct {
	networkPassphrase string
	processors        []Processor
	stats             *ProcessorStats
}

// ProcessorStats tracks processing statistics
type ProcessorStats struct {
	ProcessedAccounts int64
	CreatedAccounts   int64
	UpdatedAccounts   int64
	DeletedAccounts   int64
	LastProcessedTime time.Time
}

// NewProcessAccountData creates a new account data processor
func NewProcessAccountDataFull(config map[string]interface{}) (*ProcessAccountDataFull, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	return &ProcessAccountDataFull{
		networkPassphrase: networkPassphrase,
		processors:        make([]Processor, 0),
		stats: &ProcessorStats{
			ProcessedAccounts: 0,
			CreatedAccounts:   0,
			UpdatedAccounts:   0,
			DeletedAccounts:   0,
			LastProcessedTime: time.Time{},
		},
	}, nil
}

// AccountData represents detailed account information matching Dune's schema
type AccountDataFull struct {
	ClosedAtDate                      time.Time `json:"closed_at_date"`
	AccountID                         string    `json:"account_id"`
	Balance                           float64   `json:"balance"`
	BuyingLiabilities                 float64   `json:"buying_liabilities"`
	SellingLiabilities                float64   `json:"selling_liabilities"`
	SequenceNumber                    int64     `json:"sequence_number"`
	NumSubentries                     int64     `json:"num_subentries"`
	InflationDestination              string    `json:"inflation_destination"`
	Flags                             int64     `json:"flags"`
	HomeDomain                        string    `json:"home_domain"`
	MasterWeight                      int64     `json:"master_weight"`
	ThresholdLow                      int64     `json:"threshold_low"`
	ThresholdMedium                   int64     `json:"threshold_medium"`
	ThresholdHigh                     int64     `json:"threshold_high"`
	LastModifiedLedger                int64     `json:"last_modified_ledger"`
	LedgerEntryChange                 int64     `json:"ledger_entry_change"`
	Deleted                           bool      `json:"deleted"`
	Sponsor                           string    `json:"sponsor"`
	NumSponsored                      int64     `json:"num_sponsored"`
	NumSponsoring                     int64     `json:"num_sponsoring"`
	SequenceTime                      time.Time `json:"sequence_time"`
	ClosedAt                          time.Time `json:"closed_at"`
	LedgerSequence                    int64     `json:"ledger_sequence"`
	AccountSequenceLastModifiedLedger int64     `json:"account_sequence_last_modified_ledger"`
	UpdatedAt                         time.Time `json:"updated_at"`
	IngestedAt                        time.Time `json:"ingested_at"`
}

func (p *ProcessAccountDataFull) Process(ctx context.Context, msg Message) error {
	log.Printf("Starting to process message in ProcessAccountDataFull")

	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		log.Printf("Error: expected LedgerCloseMeta, got %T", msg.Payload)
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	log.Printf("Processing ledger sequence: %d", ledgerCloseMeta.LedgerSequence())

	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		log.Printf("Error creating ledger reader: %v", err)
		return fmt.Errorf("failed to create reader for ledger %v: %w", ledgerCloseMeta.LedgerSequence(), err)
	}
	defer ledgerTxReader.Close()

	for {
		tx, err := ledgerTxReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction: %v", err)
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Get changes directly from the transaction
		changes, err := tx.GetChanges()
		if err != nil {
			log.Printf("Error getting changes: %v", err)
			continue
		}

		for _, change := range changes {
			// Extract entry, change type, and deleted status
			ledgerEntry, changeType, deleted, err := ExtractEntryFromIngestChange(change)
			if err != nil {
				log.Printf("Error extracting entry: %v", err)
				continue
			}

			entry := &ledgerEntry

			// Skip if not an account type
			if entry.Data.Type != xdr.LedgerEntryTypeAccount {
				continue
			}

			// Update stats based on change type
			switch changeType {
			case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
				log.Printf("Found account creation")
				p.stats.CreatedAccounts++
			case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
				log.Printf("Found account update")
				p.stats.UpdatedAccounts++
			case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
				log.Printf("Found account deletion")
				p.stats.DeletedAccounts++
			}

			account := entry.Data.MustAccount()
			p.stats.ProcessedAccounts++

			// Create account data
			accountData := AccountDataFull{
				AccountID:          account.AccountId.Address(),
				Balance:            float64(account.Balance) / 10000000.0, // Convert from stroop to XLM
				SequenceNumber:     int64(account.SeqNum),
				NumSubentries:      int64(account.NumSubEntries),
				Flags:              int64(account.Flags),
				HomeDomain:         string(account.HomeDomain),
				MasterWeight:       int64(account.MasterKeyWeight()),
				ThresholdLow:       int64(account.ThresholdLow()),
				ThresholdMedium:    int64(account.ThresholdMedium()),
				ThresholdHigh:      int64(account.ThresholdHigh()),
				LastModifiedLedger: int64(entry.LastModifiedLedgerSeq),
				Deleted:            deleted,
				LedgerSequence:     int64(ledgerCloseMeta.LedgerSequence()),
				UpdatedAt:          time.Now().UTC(),
				IngestedAt:         time.Now().UTC(),
			}

			// Handle optional fields
			if account.InflationDest != nil {
				accountData.InflationDestination = account.InflationDest.Address()
			}

			// Handle V1 extensions (liabilities)
			if v1, ok := account.Ext.GetV1(); ok {
				liabilities := v1.Liabilities
				accountData.BuyingLiabilities = float64(liabilities.Buying) / 10000000.0
				accountData.SellingLiabilities = float64(liabilities.Selling) / 10000000.0
				accountData.NumSponsored = int64(account.NumSponsored())
				accountData.NumSponsoring = int64(account.NumSponsoring())
			}

			log.Printf("Processing account %s (balance: %f XLM, deleted: %v)",
				accountData.AccountID, accountData.Balance, accountData.Deleted)

			// Convert account data to JSON for all downstream processors
			jsonBytes, err := accountData.ToJSON()
			if err != nil {
				log.Printf("Error converting account data to JSON: %v", err)
				continue
			}

			// Forward JSON data to next processor
			for _, processor := range p.processors {
				if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
					log.Printf("Error in processor chain: %v", err)
					continue
				}
			}
		}
	}

	p.stats.LastProcessedTime = time.Now()
	log.Printf("Finished processing ledger %d, processed %d accounts (created: %d, updated: %d, deleted: %d)",
		ledgerCloseMeta.LedgerSequence(), p.stats.ProcessedAccounts, p.stats.CreatedAccounts,
		p.stats.UpdatedAccounts, p.stats.DeletedAccounts)

	return nil
}

// GetStats returns the current processor statistics
func (p *ProcessAccountDataFull) GetStats() ProcessorStats {
	return *p.stats
}

// Subscribe adds a processor to the chain
func (p *ProcessAccountDataFull) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

// Add this method to AccountDataFull struct
func (a AccountDataFull) ToJSON() ([]byte, error) {
	return json.Marshal(a)
}
