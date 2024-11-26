package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// Asset represents a Stellar asset
type Asset struct {
	Code   string
	Issuer string
	Type   string
}

// SaveAssetEnrichmentConsumer saves enriched asset data to PostgreSQL
type SaveAssetEnrichmentConsumer struct {
	db         *sql.DB
	processors []processor.Processor
}

func NewSaveAssetEnrichmentConsumer(config map[string]interface{}) (*SaveAssetEnrichmentConsumer, error) {
	connStr, ok := config["connection_string"].(string)
	if !ok {
		return nil, fmt.Errorf("missing PostgreSQL connection string")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %v", err)
	}

	return &SaveAssetEnrichmentConsumer{
		db: db,
	}, nil
}

func (c *SaveAssetEnrichmentConsumer) Subscribe(processor processor.Processor) {
	c.processors = append(c.processors, processor)
}

func (c *SaveAssetEnrichmentConsumer) Process(ctx context.Context, msg processor.Message) error {
	var enrichment processor.AssetEnrichment

	switch v := msg.Payload.(type) {
	case processor.AssetEnrichment:
		enrichment = v
	case []byte:
		if err := json.Unmarshal(v, &enrichment); err != nil {
			return fmt.Errorf("error unmarshaling enrichment: %v", err)
		}
	default:
		return fmt.Errorf("unexpected payload type: %T", msg.Payload)
	}

	// Get all assets for this issuer
	assets, err := c.getAssetsForIssuer(ctx, enrichment.Issuer)
	if err != nil {
		return fmt.Errorf("error getting assets: %v", err)
	}

	// Update each asset with the enriched data
	for _, asset := range assets {
		enrichment.Code = asset.Code
		enrichment.Type = asset.Type

		err = c.updateAssetEnrichment(ctx, enrichment)
		if err != nil {
			return fmt.Errorf("error updating asset %s-%s: %v",
				asset.Code, asset.Issuer, err)
		}
	}

	return nil
}

func (c *SaveAssetEnrichmentConsumer) getAssetsForIssuer(ctx context.Context, issuer string) ([]Asset, error) {
	query := `
		SELECT code, issuer, asset_type
		FROM assets 
		WHERE issuer = $1
	`
	rows, err := c.db.QueryContext(ctx, query, issuer)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var assets []Asset
	for rows.Next() {
		var asset Asset
		err := rows.Scan(&asset.Code, &asset.Issuer, &asset.Type)
		if err != nil {
			return nil, err
		}
		assets = append(assets, asset)
	}

	return assets, nil
}

func (c *SaveAssetEnrichmentConsumer) updateAssetEnrichment(ctx context.Context, enrichment processor.AssetEnrichment) error {
	query := `
		UPDATE assets 
		SET 
			home_domain = $1,
			auth_required = $2,
			auth_revocable = $3, 
			auth_immutable = $4,
			auth_clawback = $5,
			toml_url = $6,
			last_updated = $7
		WHERE code = $8 AND issuer = $9
	`

	result, err := c.db.ExecContext(ctx, query,
		enrichment.HomeDomain,
		enrichment.AuthRequired,
		enrichment.AuthRevocable,
		enrichment.AuthImmutable,
		enrichment.AuthClawback,
		enrichment.TomlURL,
		enrichment.LastUpdated,
		enrichment.Code,
		enrichment.Issuer,
	)
	if err != nil {
		return fmt.Errorf("failed to update asset enrichment: %v", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error checking rows affected: %v", err)
	}

	if rows == 0 {
		return fmt.Errorf("no rows updated for asset %s-%s",
			enrichment.Code, enrichment.Issuer)
	}

	return nil
}

func (c *SaveAssetEnrichmentConsumer) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
