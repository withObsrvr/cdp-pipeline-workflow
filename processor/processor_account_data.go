package processor

import (
	"context"
	"fmt"
	"time"

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
	accountChanges := getAccountChanges(*ledgerMeta)
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

		logging.Debug("Processing account change %d of %d (type: %s)",
			i+1, len(accountChanges), change.Type)

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
func processAccountChange(change xdr.LedgerEntryChange, meta xdr.LedgerCloseMeta) (*AccountRecord, error) {
	var entry xdr.LedgerEntry
	var ledgerEntryChangeType uint32

	// For removals, only Pre exists. For state/created/updated, use Post.
	deleted := false
	switch change.Type {
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		if change.Removed == nil {
			return nil, fmt.Errorf("removed change has nil Removed")
		}
		// For removed entries, we need to get the entry from the ledger key
		// This is a significant change in how removals are handled
		deleted = true
		// Since we can't get the full entry from a removal, we'll create a minimal record
		// with just the account ID and deletion status
		key := *change.Removed
		if key.Type != xdr.LedgerEntryTypeAccount {
			return nil, fmt.Errorf("non-account key encountered")
		}

		accountID, err := strkey.Encode(strkey.VersionByteAccountID, key.Account.AccountId.Ed25519[:])
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
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		if change.Created == nil {
			return nil, fmt.Errorf("created change has nil Created")
		}
		entry = *change.Created
		ledgerEntryChangeType = 1 // Created
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		if change.Updated == nil {
			return nil, fmt.Errorf("updated change has nil Updated")
		}
		entry = *change.Updated
		ledgerEntryChangeType = 2 // Updated
	case xdr.LedgerEntryChangeTypeLedgerEntryState:
		if change.State == nil {
			return nil, fmt.Errorf("state change has nil State")
		}
		entry = *change.State
		ledgerEntryChangeType = 3 // State
	default:
		return nil, fmt.Errorf("unknown change type %s", change.Type)
	}

	// If we're processing a deletion, we've already returned a minimal record
	if deleted {
		return nil, fmt.Errorf("should not reach here for deleted entries")
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
		Deleted:            deleted,
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
func getAccountChanges(meta xdr.LedgerCloseMeta) []xdr.LedgerEntryChange {
	var accountChanges []xdr.LedgerEntryChange
	var changes []xdr.LedgerEntryChange

	logging.Debug("getAccountChanges: Processing ledger %d (meta version: %d)", meta.LedgerSequence(), meta.V)

	switch meta.V {
	case 0:
		if meta.V0 != nil {
			logging.Debug("getAccountChanges: Processing V0 meta")

			// Check if TxSet exists and is not empty
			if meta.V0.TxSet.Txs != nil {
				logging.Debug("getAccountChanges: Found %d transactions in TxSet", len(meta.V0.TxSet.Txs))

				// Extract changes from transaction set
				for i := range meta.V0.TxSet.Txs {
					// Use the Operations() method to get operations
					ops := meta.V0.TxSet.Txs[i].Operations()
					logging.Debug("getAccountChanges: Transaction %d has %d operations", i+1, len(ops))
				}
			} else {
				logging.Debug("getAccountChanges: No transactions in TxSet")
			}

			// Extract changes directly from V0
			if meta.V0.TxProcessing != nil {
				logging.Debug("getAccountChanges: Found %d TxProcessing entries", len(meta.V0.TxProcessing))

				for i, txProcessing := range meta.V0.TxProcessing {
					if txProcessing.FeeProcessing != nil {
						logging.Debug("getAccountChanges: TxProcessing[%d] has %d FeeProcessing changes",
							i, len(txProcessing.FeeProcessing))
						changes = append(changes, txProcessing.FeeProcessing...)
					}

					logging.Debug("getAccountChanges: TxProcessing[%d] TxApplyProcessing version: %d",
						i, txProcessing.TxApplyProcessing.V)

					// Handle TxApplyProcessing based on its version
					switch txProcessing.TxApplyProcessing.V {
					case 1:
						v1Meta := txProcessing.TxApplyProcessing.MustV1()
						if v1Meta.Operations != nil {
							logging.Debug("getAccountChanges: V1 meta has %d operations", len(v1Meta.Operations))
							for j, opMeta := range v1Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V1 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 2:
						v2Meta := txProcessing.TxApplyProcessing.MustV2()
						if v2Meta.Operations != nil {
							logging.Debug("getAccountChanges: V2 meta has %d operations", len(v2Meta.Operations))
							for j, opMeta := range v2Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V2 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 3:
						v3Meta := txProcessing.TxApplyProcessing.MustV3()
						if v3Meta.Operations != nil {
							logging.Debug("getAccountChanges: V3 meta has %d operations", len(v3Meta.Operations))
							for j, opMeta := range v3Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V3 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 4:
						v4Meta := txProcessing.TxApplyProcessing.MustV4()
						if v4Meta.Operations != nil {
							logging.Debug("getAccountChanges: V4 meta has %d operations", len(v4Meta.Operations))
							for j, opMeta := range v4Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V4 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
						// V4 also includes Soroban transaction data
						if v4Meta.SorobanMeta != nil {
							logging.Debug("getAccountChanges: V4 has Soroban meta")
						}
					default:
						logging.Debug("getAccountChanges: Unknown TxApplyProcessing version: %d",
							txProcessing.TxApplyProcessing.V)
					}
				}
			} else {
				logging.Debug("getAccountChanges: No TxProcessing entries found")
			}
		}
	case 1:
		if meta.V1 != nil {
			logging.Debug("getAccountChanges: Processing V1 meta")

			// Process TxProcessing entries
			if meta.V1.TxProcessing != nil {
				logging.Debug("getAccountChanges: Found %d TxProcessing entries in V1 meta", len(meta.V1.TxProcessing))

				for i, txProcessing := range meta.V1.TxProcessing {
					// Process fee changes
					if txProcessing.FeeProcessing != nil {
						logging.Debug("getAccountChanges: V1 TxProcessing[%d] has %d FeeProcessing changes",
							i, len(txProcessing.FeeProcessing))
						changes = append(changes, txProcessing.FeeProcessing...)
					}

					// Process transaction metadata based on version
					logging.Debug("getAccountChanges: V1 TxProcessing[%d] TxApplyProcessing version: %d",
						i, txProcessing.TxApplyProcessing.V)

					switch txProcessing.TxApplyProcessing.V {
					case 1:
						v1Meta := txProcessing.TxApplyProcessing.MustV1()
						if v1Meta.Operations != nil {
							logging.Debug("getAccountChanges: V1 meta has %d operations", len(v1Meta.Operations))
							for j, opMeta := range v1Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V1 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 2:
						v2Meta := txProcessing.TxApplyProcessing.MustV2()
						if v2Meta.Operations != nil {
							logging.Debug("getAccountChanges: V2 meta has %d operations", len(v2Meta.Operations))
							for j, opMeta := range v2Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V2 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 3:
						v3Meta := txProcessing.TxApplyProcessing.MustV3()
						if v3Meta.Operations != nil {
							logging.Debug("getAccountChanges: V3 meta has %d operations", len(v3Meta.Operations))
							for j, opMeta := range v3Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V3 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 4:
						v4Meta := txProcessing.TxApplyProcessing.MustV4()
						if v4Meta.Operations != nil {
							logging.Debug("getAccountChanges: V4 meta has %d operations", len(v4Meta.Operations))
							for j, opMeta := range v4Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V4 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
						// V4 also includes Soroban transaction data
						if v4Meta.SorobanMeta != nil {
							logging.Debug("getAccountChanges: V4 has Soroban meta")
						}
					default:
						logging.Debug("getAccountChanges: Unknown TxApplyProcessing version: %d",
							txProcessing.TxApplyProcessing.V)
					}
				}
			} else {
				logging.Debug("getAccountChanges: No TxProcessing entries found in V1 meta")
			}

			// Try to access changes directly from V1 if available
			// This is a more exploratory approach since we don't know the exact structure
			logging.Debug("getAccountChanges: Exploring V1 meta structure for changes")

			// Inspect the V1 structure to find any fields that might contain changes
			// This is a debugging step to help understand the structure
			logging.Debug("getAccountChanges: V1 TxSet type: %T", meta.V1.TxSet)

			// Instead of comparing TxSet with nil, let's check if it has any transactions
			// by examining its fields or using reflection
			if meta.V1.TxSet.V == 0 {
				logging.Debug("getAccountChanges: V1 TxSet is legacy format (V=0)")
			} else if meta.V1.TxSet.V == 1 {
				logging.Debug("getAccountChanges: V1 TxSet is generalized format (V=1)")
			}

			// Add more exploration of the V1 structure as needed
		}
	case 2:
		if meta.V2 != nil {
			logging.Debug("getAccountChanges: Processing V2 meta")
			
			// Process TxProcessing entries
			if meta.V2.TxProcessing != nil {
				logging.Debug("getAccountChanges: Found %d TxProcessing entries in V2 meta", len(meta.V2.TxProcessing))
				
				for i, txProcessing := range meta.V2.TxProcessing {
					// Process fee changes
					if txProcessing.FeeProcessing != nil {
						logging.Debug("getAccountChanges: V2 TxProcessing[%d] has %d FeeProcessing changes",
							i, len(txProcessing.FeeProcessing))
						changes = append(changes, txProcessing.FeeProcessing...)
					}
					
					// Process transaction metadata based on version
					logging.Debug("getAccountChanges: V2 TxProcessing[%d] TxApplyProcessing version: %d",
						i, txProcessing.TxApplyProcessing.V)
					
					switch txProcessing.TxApplyProcessing.V {
					case 1:
						v1Meta := txProcessing.TxApplyProcessing.MustV1()
						if v1Meta.Operations != nil {
							logging.Debug("getAccountChanges: V1 meta has %d operations", len(v1Meta.Operations))
							for j, opMeta := range v1Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V1 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 2:
						v2Meta := txProcessing.TxApplyProcessing.MustV2()
						if v2Meta.Operations != nil {
							logging.Debug("getAccountChanges: V2 meta has %d operations", len(v2Meta.Operations))
							for j, opMeta := range v2Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V2 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 3:
						v3Meta := txProcessing.TxApplyProcessing.MustV3()
						if v3Meta.Operations != nil {
							logging.Debug("getAccountChanges: V3 meta has %d operations", len(v3Meta.Operations))
							for j, opMeta := range v3Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V3 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 4:
						v4Meta := txProcessing.TxApplyProcessing.MustV4()
						if v4Meta.Operations != nil {
							logging.Debug("getAccountChanges: V4 meta has %d operations", len(v4Meta.Operations))
							for j, opMeta := range v4Meta.Operations {
								if opMeta.Changes != nil {
									logging.Debug("getAccountChanges: V4 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
						// V4 also includes Soroban transaction data
						if v4Meta.SorobanMeta != nil {
							logging.Debug("getAccountChanges: V4 has Soroban meta")
						}
					default:
						logging.Debug("getAccountChanges: Unknown TxApplyProcessing version: %d",
							txProcessing.TxApplyProcessing.V)
					}
				}
			} else {
				logging.Debug("getAccountChanges: No TxProcessing entries found in V2 meta")
			}
		}
	default:
		logging.Debug("getAccountChanges: Unknown meta version: %d", meta.V)
	}

	logging.Debug("getAccountChanges: Found %d total changes", len(changes))

	// Filter for account changes
	for i, change := range changes {
		logging.Debug("getAccountChanges: Examining change %d of type %s", i+1, change.Type)

		// Check if this is an account entry
		var entry *xdr.LedgerEntry

		switch change.Type {
		case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
			entry = change.Created
			logging.Debug("getAccountChanges: Found Created entry")
		case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
			entry = change.Updated
			logging.Debug("getAccountChanges: Found Updated entry")
		case xdr.LedgerEntryChangeTypeLedgerEntryState:
			entry = change.State
			logging.Debug("getAccountChanges: Found State entry")
		case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
			logging.Debug("getAccountChanges: Found Removed entry")
			// For removals, we need to check the key type
			if change.Removed != nil && change.Removed.Type == xdr.LedgerEntryTypeAccount {
				logging.Debug("getAccountChanges: Found account removal!")
				accountChanges = append(accountChanges, change)
			}
			continue
		default:
			logging.Debug("getAccountChanges: Unknown change type: %s", change.Type)
			continue
		}

		if entry != nil {
			logging.Debug("getAccountChanges: Entry type: %s", entry.Data.Type)
			if entry.Data.Type == xdr.LedgerEntryTypeAccount {
				logging.Debug("getAccountChanges: Found account entry!")
				accountChanges = append(accountChanges, change)
			}
		} else {
			logging.Debug("getAccountChanges: Entry is nil")
		}
	}

	logging.Debug("getAccountChanges: Returning %d account changes", len(accountChanges))
	return accountChanges
}
