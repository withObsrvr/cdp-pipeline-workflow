package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveClaimableBalanceToPostgres struct {
	db *pgxpool.Pool
}

func NewSaveClaimableBalanceToPostgres(config map[string]interface{}) (*SaveClaimableBalanceToPostgres, error) {
	dbURL, ok := config["database_url"].(string)
	if !ok {
		return nil, fmt.Errorf("database_url is required")
	}

	db, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	// Create tables if they don't exist
	if err := createTables(context.Background(), db); err != nil {
		return nil, fmt.Errorf("error creating tables: %w", err)
	}

	return &SaveClaimableBalanceToPostgres{
		db: db,
	}, nil
}

func (s *SaveClaimableBalanceToPostgres) Process(ctx context.Context, msg processor.Message) error {
	var event processor.ClaimableBalanceEvent
	if err := json.Unmarshal(msg.Payload.([]byte), &event); err != nil {
		return fmt.Errorf("error unmarshaling event: %w", err)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert or update the claimable balance
	if err := s.upsertClaimableBalance(ctx, tx, &event); err != nil {
		return fmt.Errorf("error upserting claimable balance: %w", err)
	}

	// Handle claimants if the balance is not deleted
	if !event.Deleted {
		if err := s.updateClaimants(ctx, tx, &event); err != nil {
			return fmt.Errorf("error updating claimants: %w", err)
		}
	}

	return tx.Commit(ctx)
}

func (s *SaveClaimableBalanceToPostgres) Close() error {
	if s.db != nil {
		s.db.Close()
	}
	return nil
}

func createTables(ctx context.Context, db *pgxpool.Pool) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS claimable_balances (
            balance_id TEXT PRIMARY KEY,
            asset_code TEXT,
            asset_issuer TEXT,
            asset_type TEXT,
            asset_id BIGINT,
            asset_amount DOUBLE PRECISION,
            sponsor TEXT,
            flags INTEGER,
            last_modified_ledger INTEGER,
            ledger_sequence INTEGER,
            deleted BOOLEAN,
            closed_at TIMESTAMP,
            tx_hash TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
		`CREATE TABLE IF NOT EXISTS claimable_balance_claimants (
            id SERIAL PRIMARY KEY,
            balance_id TEXT REFERENCES claimable_balances(balance_id),
            destination TEXT,
            predicate JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(balance_id, destination)
        )`,
		`CREATE INDEX IF NOT EXISTS idx_claimable_balances_asset ON claimable_balances(asset_code, asset_issuer)`,
		`CREATE INDEX IF NOT EXISTS idx_claimable_balances_deleted ON claimable_balances(deleted)`,
	}

	for _, query := range queries {
		if _, err := db.Exec(ctx, query); err != nil {
			return fmt.Errorf("error executing query: %w", err)
		}
	}
	return nil
}

func (s *SaveClaimableBalanceToPostgres) upsertClaimableBalance(ctx context.Context, tx pgx.Tx, event *processor.ClaimableBalanceEvent) error {
	query := `
        INSERT INTO claimable_balances (
            balance_id, asset_code, asset_issuer, asset_type, asset_id,
            asset_amount, sponsor, flags, last_modified_ledger,
            ledger_sequence, deleted, closed_at, tx_hash, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
        ON CONFLICT (balance_id) DO UPDATE SET
            asset_amount = EXCLUDED.asset_amount,
            sponsor = EXCLUDED.sponsor,
            flags = EXCLUDED.flags,
            last_modified_ledger = EXCLUDED.last_modified_ledger,
            ledger_sequence = EXCLUDED.ledger_sequence,
            deleted = EXCLUDED.deleted,
            closed_at = EXCLUDED.closed_at,
            tx_hash = EXCLUDED.tx_hash,
            updated_at = NOW()`

	_, err := tx.Exec(ctx, query,
		event.BalanceID,
		event.AssetCode,
		event.AssetIssuer,
		event.AssetType,
		event.AssetID,
		event.AssetAmount,
		event.Sponsor,
		event.Flags,
		event.LastModifiedLedger,
		event.LedgerSequence,
		event.Deleted,
		event.ClosedAt,
		event.TxHash,
	)

	if err != nil {
		return fmt.Errorf("error upserting claimable balance: %w", err)
	}

	return nil
}

func (s *SaveClaimableBalanceToPostgres) updateClaimants(ctx context.Context, tx pgx.Tx, event *processor.ClaimableBalanceEvent) error {
	// First, delete existing claimants for this balance
	_, err := tx.Exec(ctx, `DELETE FROM claimable_balance_claimants WHERE balance_id = $1`, event.BalanceID)
	if err != nil {
		return fmt.Errorf("error deleting existing claimants: %w", err)
	}

	// Then insert the new claimants
	for _, claimant := range event.Claimants {
		predicateJSON, err := json.Marshal(claimant.Predicate)
		if err != nil {
			return fmt.Errorf("error marshaling predicate: %w", err)
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO claimable_balance_claimants (
                balance_id, destination, predicate
            ) VALUES ($1, $2, $3)`,
			event.BalanceID,
			claimant.Destination,
			predicateJSON,
		)
		if err != nil {
			return fmt.Errorf("error inserting claimant: %w", err)
		}
	}

	return nil
}
