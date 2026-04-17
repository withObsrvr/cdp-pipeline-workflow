package processor

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// AssetEnrichment represents the enriched asset data
type AssetEnrichment struct {
	Code          string    `json:"code"`
	Issuer        string    `json:"issuer"`
	HomeDomain    string    `json:"home_domain"`
	AuthRequired  bool      `json:"auth_required"`
	AuthRevocable bool      `json:"auth_revocable"`
	AuthImmutable bool      `json:"auth_immutable"`
	AuthClawback  bool      `json:"auth_clawback"`
	LastUpdated   time.Time `json:"last_updated"`
	TomlURL       string    `json:"toml_url"`
	Type          string    `json:"type"`
	OperationType string    `json:"operation_type"`
}

// AssetEnrichmentProcessor processes ledger entries to enrich asset data
type AssetEnrichmentProcessor struct {
	processors        []Processor
	db                *sql.DB
	networkPassphrase string
}

func NewAssetEnrichmentProcessor(config map[string]interface{}) (*AssetEnrichmentProcessor, error) {
	connStr, ok := config["connection_string"].(string)
	if !ok {
		return nil, fmt.Errorf("missing PostgreSQL connection string")
	}

	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network passphrase")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %v", err)
	}

	return &AssetEnrichmentProcessor{
		db:                db,
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *AssetEnrichmentProcessor) getAssetsForIssuer(ctx context.Context, issuer string) ([]Asset, error) {
	// Add timeout to prevent long-running queries
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	// Create a defer function that checks context before closing
	defer func() {
		// If context is already canceled, don't wait for cleanup
		if ctx.Err() != nil {
			cancel()
			return
		}
		// Give cleanup operations a short timeout
		_, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		cancel()
	}()

	query := `
		SELECT code, issuer, asset_type
		FROM assets 
		WHERE issuer = $1
	`
	rows, err := p.db.QueryContext(queryCtx, query, issuer)
	if err != nil {
		return nil, fmt.Errorf("database query failed: %w", err)
	}

	// Ensure rows are closed even if context is canceled
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()

	var assets []Asset
	for rows.Next() {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context canceled during iteration: %w", ctx.Err())
		default:
			var asset Asset
			if err := rows.Scan(&asset.Code, &asset.Issuer, &asset.Type); err != nil {
				return nil, fmt.Errorf("error scanning row: %w", err)
			}
			assets = append(assets, asset)
		}
	}

	// Check for errors after iteration
	if err = rows.Err(); err != nil {
		if err == context.Canceled {
			return nil, fmt.Errorf("query canceled: %w", err)
		}
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return assets, nil
}

func (p *AssetEnrichmentProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	// Read ledger transactions
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %v", err)
	}
	defer txReader.Close()

	// Process all transactions in the ledger
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Process each operation in the transaction
		for _, op := range tx.Envelope.Operations() {
			// Get the source account for this operation
			var sourceAccount string
			if op.SourceAccount != nil {
				sourceAccount = op.SourceAccount.Address()
			} else {
				muxedAccount, ok := tx.Envelope.SourceAccount().GetMed25519()
				if ok {
					sourceAccount, _ = strkey.Encode(strkey.VersionByteAccountID, muxedAccount.Ed25519[:])
				}
			}

			// Fetch all assets issued by this account
			assets, err := p.getAssetsForIssuer(ctx, sourceAccount)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					// If context is canceled, stop processing entirely
					return fmt.Errorf("context canceled while processing: %w", ctxErr)
				}
				// For other errors, log and continue
				log.Printf("Warning: Error fetching assets for issuer %s: %v", sourceAccount, err)
				continue
			}

			// Skip if no assets found for this issuer
			if len(assets) == 0 {
				continue
			}

			// Get account entry from transaction metadata
			var accountEntry *xdr.AccountEntry
			if tx.UnsafeMeta.V1 != nil && len(tx.UnsafeMeta.V1.Operations) > 0 {
				for _, change := range tx.UnsafeMeta.V1.Operations[0].Changes {
					if change.Type == xdr.LedgerEntryChangeTypeLedgerEntryState {
						if change.State != nil && change.State.Data.Type == xdr.LedgerEntryTypeAccount {
							if acc, ok := change.State.Data.GetAccount(); ok {
								if acc.AccountId.Address() == sourceAccount {
									accountEntry = &acc
									break
								}
							}
						}
					}
				}
			}

			if accountEntry == nil {
				continue
			}

			// Create enrichment data for each asset
			for _, asset := range assets {
				homeDomain := string(accountEntry.HomeDomain)

				enrichment := AssetEnrichment{
					Code:          asset.Code,
					Issuer:        asset.Issuer,
					Type:          asset.Type,
					HomeDomain:    homeDomain,
					AuthRequired:  (uint32(accountEntry.Flags) & uint32(xdr.AccountFlagsAuthRequiredFlag)) != 0,
					AuthRevocable: (uint32(accountEntry.Flags) & uint32(xdr.AccountFlagsAuthRevocableFlag)) != 0,
					AuthImmutable: (uint32(accountEntry.Flags) & uint32(xdr.AccountFlagsAuthImmutableFlag)) != 0,
					AuthClawback:  (uint32(accountEntry.Flags) & uint32(xdr.AccountFlagsAuthClawbackEnabledFlag)) != 0,
					LastUpdated:   time.Now(),
					OperationType: "asset_enrichment",
				}

				if homeDomain != "" {
					enrichment.TomlURL = fmt.Sprintf("https://%s/.well-known/stellar.toml", homeDomain)
				}

				// Forward enriched data to consumers
				for _, processor := range p.processors {
					if err := processor.Process(ctx, Message{Payload: enrichment}); err != nil {
						log.Printf("Error processing enrichment for asset %s-%s: %v",
							asset.Code, asset.Issuer, err)
					}
				}
			}
		}
	}

	return nil
}

func (p *AssetEnrichmentProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *AssetEnrichmentProcessor) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
