package processor

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/logging"
)

// AccountDataConfig holds the configuration for the account data processor
type AccountDataConfig struct {
	NetworkPassphrase string
	Processors        []Processor
}

// AccountRecord represents the transformed account data.
type AccountRecord struct {
	AccountID          string    `json:"account_id"`
	Balance            string    `json:"balance"`             // In stroops
	BuyingLiabilities  string    `json:"buying_liabilities"`  // In stroops
	SellingLiabilities string    `json:"selling_liabilities"` // In stroops
	Sequence           uint64    `json:"sequence"`            // Sequence number
	SequenceLedger     uint32    `json:"sequence_ledger"`     // Ledger where sequence was last modified
	SequenceTime       time.Time `json:"sequence_time"`       // Time when sequence was last modified
	NumSubentries      uint32    `json:"num_subentries"`
	InflationDest      string    `json:"inflation_dest,omitempty"`
	Flags              uint32    `json:"flags"` // Account flags
	HomeDomain         string    `json:"home_domain,omitempty"`
	MasterWeight       uint32    `json:"master_weight"`
	LowThreshold       uint32    `json:"low_threshold"`
	MediumThreshold    uint32    `json:"medium_threshold"`
	HighThreshold      uint32    `json:"high_threshold"`
	Sponsor            string    `json:"sponsor,omitempty"` // Account sponsor
	NumSponsored       uint32    `json:"num_sponsored"`     // Number of entries this account sponsors
	NumSponsoring      uint32    `json:"num_sponsoring"`    // Number of entries sponsoring this account
	LastModifiedLedger uint32    `json:"last_modified_ledger"`
	LedgerEntryChange  uint32    `json:"ledger_entry_change"` // Type of ledger entry change
	Deleted            bool      `json:"deleted"`
	ClosedAt           time.Time `json:"closed_at"`       // Time when the ledger closed
	LedgerSequence     uint32    `json:"ledger_sequence"` // Ledger sequence number
	Timestamp          time.Time `json:"timestamp"`       // Processing timestamp
}

// ProcessAccountData implements the Processor interface for account data.
type ProcessAccountData struct {
	networkPassphrase string
	processors        []Processor
	stats             struct {
		ProcessedAccounts int64
		CreatedAccounts   int64
		UpdatedAccounts   int64
		DeletedAccounts   int64
		LastProcessedTime time.Time
	}
}

// NewProcessAccountData creates a new ProcessAccountData processor.
func NewProcessAccountData(config map[string]interface{}) (*ProcessAccountData, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	return &ProcessAccountData{
		networkPassphrase: networkPassphrase,
		processors:        make([]Processor, 0),
	}, nil
}

// Subscribe adds a processor to the chain.
func (p *ProcessAccountData) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

// Process processes a message containing a LedgerCloseMeta and extracts account changes.
func (p *ProcessAccountData) Process(ctx context.Context, msg Message) error {
	// Create a new context with a longer timeout for the entire ledger processing
	processingCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	// Extract LedgerCloseMeta
	ledgerMeta, ok := msg.Payload.(*xdr.LedgerCloseMeta)
	if !ok {
		// Check if it's already a pass-through message
		if lcm, ok := msg.Payload.(xdr.LedgerCloseMeta); ok {
			ledgerMeta = &lcm
		} else {
			return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
		}
	}

	logging.Debug("Processing ledger %d for account data", ledgerMeta.LedgerSequence())

	// Extract account changes from ledgerMeta
	accountChanges, err := getAccountChanges(p.networkPassphrase, *ledgerMeta)
	if err != nil {
		return fmt.Errorf("error extracting account changes from ledger %d: %w", ledgerMeta.LedgerSequence(), err)
	}
	logging.Debug("Found %d account changes in ledger %d", len(accountChanges), ledgerMeta.LedgerSequence())

	// Track errors but don't fail immediately
	var processingErrors []error
	var accountRecords []AccountRecord

	// Process each account change.
	for i, change := range accountChanges {
		// Check if the context has been canceled
		if processingCtx.Err() != nil {
			return fmt.Errorf("processing interrupted: %w", processingCtx.Err())
		}

		logging.Debug("Processing account change %d of %d (change: %s, reason: %d)",
			i+1, len(accountChanges), change.ChangeType, change.Reason)

		record, err := processAccountChange(change, *ledgerMeta)
		if err != nil {
			logging.Debug("Error processing account change: %v", err)
			processingErrors = append(processingErrors, fmt.Errorf("error processing change %d: %w", i+1, err))
			continue
		}

		logging.Debug("Successfully processed account %s (deleted: %t)",
			record.AccountID, record.Deleted)

		// Update stats
		p.stats.ProcessedAccounts++
		if record.LedgerEntryChange == 1 {
			p.stats.CreatedAccounts++
		} else if record.LedgerEntryChange == 2 {
			p.stats.UpdatedAccounts++
		} else if record.Deleted {
			p.stats.DeletedAccounts++
		}

		accountRecords = append(accountRecords, *record)
	}

	p.stats.LastProcessedTime = time.Now()
	logging.Debug("Finished processing ledger %d, processed %d accounts (created: %d, updated: %d, deleted: %d)",
		ledgerMeta.LedgerSequence(), p.stats.ProcessedAccounts, p.stats.CreatedAccounts,
		p.stats.UpdatedAccounts, p.stats.DeletedAccounts)

	// Create new message with original payload and updated metadata
	outputMsg := Message{
		Payload:  ledgerMeta,
		Metadata: msg.Metadata,
	}

	// Initialize metadata if nil
	if outputMsg.Metadata == nil {
		outputMsg.Metadata = make(map[string]interface{})
	}

	// Add account records to metadata
	outputMsg.Metadata["accounts"] = accountRecords
	outputMsg.Metadata["processor_account_data"] = true

	// Forward to subscribers
	logging.Debug("Forwarding %d account records to %d processors",
		len(accountRecords), len(p.processors))

	for i, processor := range p.processors {
		logging.Debug("ProcessAccountData: Forwarding to processor %d/%d (%T)", i+1, len(p.processors), processor)
		// Create a separate context for each processor with a reasonable timeout
		procCtx, procCancel := context.WithTimeout(processingCtx, 30*time.Second)

		logging.Debug("ProcessAccountData: About to call Process on %T", processor)
		err := processor.Process(procCtx, outputMsg)
		logging.Debug("ProcessAccountData: Process call returned for %T, err=%v", processor, err)
		procCancel() // Cancel the context immediately after use

		if err != nil {
			logging.Debug("Error forwarding to processor %d (%T): %v", i+1, processor, err)
			processingErrors = append(processingErrors, fmt.Errorf("error in processor: %w", err))
		} else {
			logging.Debug("Successfully forwarded to processor %d (%T)", i+1, processor)
		}
	}

	// If we had any errors, return a combined error
	if len(processingErrors) > 0 {
		return fmt.Errorf("encountered %d errors while processing ledger %d", len(processingErrors), ledgerMeta.LedgerSequence())
	}

	return nil
}

// GetStats returns the current processor statistics
func (p *ProcessAccountData) GetStats() struct {
	ProcessedAccounts int64
	CreatedAccounts   int64
	UpdatedAccounts   int64
	DeletedAccounts   int64
	LastProcessedTime time.Time
} {
	return p.stats
}

// processAccountChange converts a single LedgerEntryChange into an AccountRecord.
func processAccountChange(change ingest.Change, meta xdr.LedgerCloseMeta) (*AccountRecord, error) {
	var entry xdr.LedgerEntry
	var ledgerEntryChangeType uint32

	// An ingest.Change is normalised to a Pre/Post pair: no Post means the entry
	// was removed, no Pre means it was created.
	switch {
	case change.Pre != nil && change.Post == nil:
		// Removals only ever produced a minimal record here, keyed by account ID.
		accountEntry, ok := change.Pre.Data.GetAccount()
		if !ok {
			return nil, fmt.Errorf("non-account entry encountered")
		}
		if accountEntry.AccountId.Ed25519 == nil {
			return nil, fmt.Errorf("account ID is nil")
		}

		accountID, err := strkey.Encode(strkey.VersionByteAccountID, accountEntry.AccountId.Ed25519[:])
		if err != nil {
			return nil, fmt.Errorf("error encoding account ID: %w", err)
		}

		// Get ledger close time
		var closedAt time.Time
		if meta.LedgerSequence() > 0 {
			closedAt = time.Unix(int64(meta.LedgerCloseTime()), 0).UTC()
		} else {
			closedAt = time.Now().UTC()
		}

		return &AccountRecord{
			AccountID:      accountID,
			Deleted:        true,
			ClosedAt:       closedAt,
			LedgerSequence: uint32(meta.LedgerSequence()),
			Timestamp:      time.Now(),
		}, nil
	case change.Pre == nil && change.Post != nil:
		entry = *change.Post
		ledgerEntryChangeType = 1 // Created
	case change.Pre != nil && change.Post != nil:
		entry = *change.Post
		ledgerEntryChangeType = 2 // Updated
	default:
		return nil, fmt.Errorf("change has neither a pre nor a post state")
	}

	// Ensure this is an Account entry.
	if entry.Data.Type != xdr.LedgerEntryTypeAccount {
		return nil, fmt.Errorf("non-account entry encountered")
	}

	accountEntry := entry.Data.MustAccount()

	// Convert account ID using strkey.
	var accountIdBytes []byte
	if accountEntry.AccountId.Ed25519 != nil {
		accountIdBytes = accountEntry.AccountId.Ed25519[:]
	} else {
		return nil, fmt.Errorf("account ID is nil")
	}

	accountID, err := strkey.Encode(strkey.VersionByteAccountID, accountIdBytes)
	if err != nil {
		return nil, fmt.Errorf("error encoding account ID: %w", err)
	}

	// Get sponsor if available
	var sponsor string
	if entry.Ext.V == 1 && entry.Ext.V1 != nil && entry.Ext.V1.SponsoringId != nil {
		sponsorBytes := entry.Ext.V1.SponsoringId.Ed25519[:]
		sponsor, err = strkey.Encode(strkey.VersionByteAccountID, sponsorBytes)
		if err != nil {
			logging.Debug("Warning: Could not encode sponsor ID: %v", err)
		}
	}

	// Get ledger close time
	var closedAt time.Time
	if meta.LedgerSequence() > 0 {
		closedAt = time.Unix(int64(meta.LedgerCloseTime()), 0).UTC()
	} else {
		closedAt = time.Now().UTC()
	}

	// Build the AccountRecord.
	record := &AccountRecord{
		AccountID:          accountID,
		Balance:            fmt.Sprintf("%d", accountEntry.Balance),
		BuyingLiabilities:  fmt.Sprintf("%d", accountEntry.Liabilities().Buying),
		SellingLiabilities: fmt.Sprintf("%d", accountEntry.Liabilities().Selling),
		Sequence:           uint64(accountEntry.SeqNum),
		SequenceLedger:     uint32(entry.LastModifiedLedgerSeq), // Using last modified as a proxy
		SequenceTime:       closedAt,                            // Using ledger close time as a proxy
		NumSubentries:      uint32(accountEntry.NumSubEntries),
		Flags:              uint32(accountEntry.Flags),
		MasterWeight:       uint32(accountEntry.Thresholds[0]),
		LowThreshold:       uint32(accountEntry.Thresholds[1]),
		MediumThreshold:    uint32(accountEntry.Thresholds[2]),
		HighThreshold:      uint32(accountEntry.Thresholds[3]),
		Sponsor:            sponsor,
		NumSponsored:       uint32(accountEntry.NumSponsored()),
		NumSponsoring:      uint32(accountEntry.NumSponsoring()),
		LastModifiedLedger: uint32(entry.LastModifiedLedgerSeq),
		LedgerEntryChange:  ledgerEntryChangeType,
		Deleted:            false, // removals return a minimal record above
		ClosedAt:           closedAt,
		LedgerSequence:     uint32(meta.LedgerSequence()),
		Timestamp:          time.Now(),
	}

	// Set InflationDest if present.
	if accountEntry.InflationDest != nil {
		var inflationDestBytes []byte
		if accountEntry.InflationDest.Ed25519 != nil {
			inflationDestBytes = accountEntry.InflationDest.Ed25519[:]
			inflationDest, err := strkey.Encode(strkey.VersionByteAccountID, inflationDestBytes)
			if err != nil {
				logging.Debug("Warning: Could not encode inflation destination: %v", err)
			} else {
				record.InflationDest = inflationDest
			}
		}
	}

	// Set HomeDomain if present.
	homeDomain := string(accountEntry.HomeDomain)
	if homeDomain != "" {
		record.HomeDomain = homeDomain
	}

	return record, nil
}

// getAccountChanges extracts all account-related changes from the LedgerCloseMeta.
//
// LedgerChangeReader walks the whole ledger — fee changes, transaction-level
// changes, per-operation changes, post-apply fee refunds and protocol upgrades —
// and is aware of every LedgerCloseMeta and TransactionMeta version. It replaces
// a hand-rolled walk that had to enumerate each version pair by hand and which
// only ever looked at fee processing and operation meta.
func getAccountChanges(networkPassphrase string, meta xdr.LedgerCloseMeta) ([]ingest.Change, error) {
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(networkPassphrase, meta)
	if err != nil {
		return nil, fmt.Errorf("error creating ledger change reader: %w", err)
	}
	defer reader.Close()

	var accountChanges []ingest.Change
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading ledger changes: %w", err)
		}

		if change.Type == xdr.LedgerEntryTypeAccount {
			accountChanges = append(accountChanges, change)
		}
	}

	logging.Debug("getAccountChanges: Returning %d account changes", len(accountChanges))
	return accountChanges, nil
}
