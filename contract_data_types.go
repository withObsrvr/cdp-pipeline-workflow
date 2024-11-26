package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// Type definitions
type ContractDataOutput struct {
	ContractId                string    `json:"contract_id"`
	ContractKeyType           string    `json:"contract_key_type"`
	ContractDurability        string    `json:"contract_durability"`
	ContractDataAssetCode     string    `json:"asset_code"`
	ContractDataAssetIssuer   string    `json:"asset_issuer"`
	ContractDataAssetType     string    `json:"asset_type"`
	ContractDataBalanceHolder string    `json:"balance_holder"`
	ContractDataBalance       string    `json:"balance"`
	LastModifiedLedger        uint32    `json:"last_modified_ledger"`
	LedgerEntryChange         uint32    `json:"ledger_entry_change"`
	Deleted                   bool      `json:"deleted"`
	ClosedAt                  time.Time `json:"closed_at"`
	LedgerSequence            uint32    `json:"ledger_sequence"`
	LedgerKeyHash             string    `json:"ledger_key_hash"`
}

type TransformContractDataStruct struct {
	AssetFromContractData           func(ledgerEntry xdr.LedgerEntry, passphrase string) *xdr.Asset
	ContractBalanceFromContractData func(ledgerEntry xdr.LedgerEntry, passphrase string) ([32]byte, *big.Int, bool)
}

// Helper functions
func loadWatchlist(path string) (map[string]bool, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read watchlist file: %w", err)
	}

	var watchlist []struct {
		ContractID string `json:"contract_id"`
	}
	if err := json.Unmarshal(file, &watchlist); err != nil {
		return nil, fmt.Errorf("failed to parse watchlist JSON: %w", err)
	}

	contracts := make(map[string]bool)
	for _, item := range watchlist {
		contracts[item.ContractID] = true
	}

	return contracts, nil
}

func initializeDatabase(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Create tables
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS contract_data (
			contract_id VARCHAR,
			contract_key_type VARCHAR,
			contract_durability VARCHAR,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			asset_type VARCHAR,
			balance_holder VARCHAR,
			balance VARCHAR,
			last_modified_ledger INTEGER,
			ledger_entry_change INTEGER,
			deleted BOOLEAN,
			closed_at TIMESTAMP,
			ledger_sequence INTEGER,
			ledger_key_hash VARCHAR,
			PRIMARY KEY (contract_id, ledger_sequence, ledger_key_hash)
		);

		CREATE INDEX IF NOT EXISTS idx_contract_data_ledger 
		ON contract_data(ledger_sequence);
		
		CREATE INDEX IF NOT EXISTS idx_contract_data_contract 
		ON contract_data(contract_id);
	`)

	if err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return db, nil
}

func NewTransformContractDataStruct(
	assetFrom func(ledgerEntry xdr.LedgerEntry, passphrase string) *xdr.Asset,
	contractBalance func(ledgerEntry xdr.LedgerEntry, passphrase string) ([32]byte, *big.Int, bool),
) *TransformContractDataStruct {
	return &TransformContractDataStruct{
		AssetFromContractData:           assetFrom,
		ContractBalanceFromContractData: contractBalance,
	}
}

func ExtractEntryFromChange(change ingest.Change) (xdr.LedgerEntry, xdr.LedgerEntryChangeType, bool, error) {
	switch changeType := change.LedgerEntryChangeType(); changeType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		return *change.Post, changeType, false, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		return *change.Post, changeType, false, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return *change.Pre, changeType, true, nil
	default:
		return xdr.LedgerEntry{}, changeType, false, fmt.Errorf("unknown change type: %v", changeType)
	}
}

// Asset-related helper functions
var (
	balanceMetadataSym = xdr.ScSymbol("Balance")
	issuerSym          = xdr.ScSymbol("issuer")
	assetCodeSym       = xdr.ScSymbol("asset_code")
	assetInfoSym       = xdr.ScSymbol("AssetInfo")
	assetInfoVec       = &xdr.ScVec{
		xdr.ScVal{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &assetInfoSym,
		},
	}
	assetInfoKey = xdr.ScVal{
		Type: xdr.ScValTypeScvVec,
		Vec:  &assetInfoVec,
	}
)

func AssetFromContractData(ledgerEntry xdr.LedgerEntry, passphrase string) *xdr.Asset {
	contractData, ok := ledgerEntry.Data.GetContractData()
	if !ok {
		return nil
	}

	if contractData.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
		return nil
	}

	contractInstanceData, ok := contractData.Val.GetInstance()
	if !ok || contractInstanceData.Storage == nil {
		return nil
	}

	nativeAssetContractID, err := xdr.MustNewNativeAsset().ContractID(passphrase)
	if err != nil {
		return nil
	}

	// Find asset info in storage
	var assetInfo *xdr.ScVal
	for _, entry := range *contractInstanceData.Storage {
		if entry.Key.Equals(assetInfoKey) {
			assetInfo = &entry.Val
			break
		}
	}

	if assetInfo == nil {
		return nil
	}

	// Parse asset info
	vec, ok := assetInfo.GetVec()
	if !ok || vec == nil || len(*vec) != 2 {
		return nil
	}

	assetType, ok := (*vec)[0].GetSym()
	if !ok {
		return nil
	}

	// Handle native asset case
	if assetType == "Native" {
		if contractData.Contract.ContractId != nil &&
			*contractData.Contract.ContractId == nativeAssetContractID {
			asset := xdr.MustNewNativeAsset()
			return &asset
		}
		return nil
	}

	// Handle credit assets
	assetMap, ok := (*vec)[1].GetMap()
	if !ok || assetMap == nil || len(*assetMap) != 2 {
		return nil
	}

	// Extract asset code and issuer
	var assetCode, assetIssuer string
	for _, entry := range *assetMap {
		sym, ok := entry.Key.GetSym()
		if !ok {
			continue
		}

		switch sym {
		case assetCodeSym:
			if str, ok := entry.Val.GetStr(); ok {
				assetCode = string(str)
			}
		case issuerSym:
			if bytes, ok := entry.Val.GetBytes(); ok {
				assetIssuer, err = strkey.Encode(strkey.VersionByteAccountID, bytes)
				if err != nil {
					return nil
				}
			}
		}
	}

	if assetCode == "" || assetIssuer == "" {
		return nil
	}

	asset, err := xdr.NewCreditAsset(assetCode, assetIssuer)
	if err != nil {
		return nil
	}

	expectedID, err := asset.ContractID(passphrase)
	if err != nil {
		return nil
	}

	if contractData.Contract.ContractId == nil ||
		expectedID != *contractData.Contract.ContractId {
		return nil
	}

	return &asset
}

func ContractBalanceFromContractData(ledgerEntry xdr.LedgerEntry, passphrase string) ([32]byte, *big.Int, bool) {
	contractData, ok := ledgerEntry.Data.GetContractData()
	if !ok {
		return [32]byte{}, nil, false
	}

	// Check contract ID and key type
	if contractData.Contract.ContractId == nil {
		return [32]byte{}, nil, false
	}

	keyVec, ok := contractData.Key.GetVec()
	if !ok || keyVec == nil || len(*keyVec) != 2 {
		return [32]byte{}, nil, false
	}

	// Verify this is a balance entry
	if !(*keyVec)[0].Equals(xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &balanceMetadataSym,
	}) {
		return [32]byte{}, nil, false
	}

	// Get contract ID from address
	addr, ok := (*keyVec)[1].GetAddress()
	if !ok {
		return [32]byte{}, nil, false
	}

	holder, ok := addr.GetContractId()
	if !ok {
		return [32]byte{}, nil, false
	}

	// Parse balance map
	balanceMap, ok := contractData.Val.GetMap()
	if !ok || balanceMap == nil || len(*balanceMap) != 3 {
		return [32]byte{}, nil, false
	}

	// Extract amount
	var amount xdr.Int128Parts // Changed from *xdr.Int128Parts
	for _, entry := range *balanceMap {
		sym, ok := entry.Key.GetSym()
		if !ok || sym != "amount" {
			continue
		}
		i128, ok := entry.Val.GetI128()
		if !ok {
			return [32]byte{}, nil, false
		}
		amount = i128 // Direct assignment, no pointer handling needed
		break
	}

	// Convert to big.Int
	if int64(amount.Hi) < 0 {
		return [32]byte{}, nil, false
	}

	amt := new(big.Int).Lsh(new(big.Int).SetInt64(int64(amount.Hi)), 64)
	amt.Add(amt, new(big.Int).SetUint64(uint64(amount.Lo)))

	return holder, amt, true
}
