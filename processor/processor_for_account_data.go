package processor

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// AccountData represents key account information we want to track
type AccountData struct {
	AccountID    string `json:"account_id"`
	HomeDomain   string `json:"home_domain"`
	Flags        uint32 `json:"flags"`
	LastModified uint32 `json:"last_modified"`
}

type ProcessAccountData struct {
	networkPassphrase string
	processors        []Processor
}

func NewProcessAccountData(config map[string]interface{}) (*ProcessAccountData, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration: missing 'network_passphrase'")
	}

	return &ProcessAccountData{networkPassphrase: networkPassphrase}, nil
}

func (p *ProcessAccountData) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *ProcessAccountData) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in ProcessAccountData")
	ledgerCloseMeta := msg.Payload.(xdr.LedgerCloseMeta)
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("failed to create reader for ledger %v: %w", ledgerCloseMeta.LedgerSequence(), err)
	}
	defer ledgerTxReader.Close()

	for {
		tx, err := ledgerTxReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Get the transaction metadata
		if v1Meta, ok := tx.UnsafeMeta.GetV1(); !ok {
			continue
		} else {
			// Process each operation's changes
			for i := range v1Meta.Operations {
				changes := v1Meta.Operations[i].Changes
				if changes == nil {
					continue
				}

				for _, change := range changes {
					// Check if the change is a Created or Updated entry
					var entry *xdr.LedgerEntry
					switch change.Type {
					case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
						entry = change.Created
					case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
						entry = change.Updated
					}

					// Skip if no entry or not an account type
					if entry == nil || entry.Data.Type != xdr.LedgerEntryTypeAccount {
						continue
					}

					// Process account data
					account := entry.Data.Account

					accountData := AccountData{
						AccountID:    account.AccountId.Address(),
						HomeDomain:   string(account.HomeDomain),
						Flags:        uint32(account.Flags),
						LastModified: uint32(entry.LastModifiedLedgerSeq),
					}

					// Forward account data to next processor
					for _, processor := range p.processors {
						if err := processor.Process(ctx, Message{Payload: accountData}); err != nil {
							log.Printf("Error in processor chain: %v", err)
							continue
						}
					}
				}
			}
		}
	}

	return nil
}
