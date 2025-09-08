package processor

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

var (
	// Storage DataKey enum constants
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
	transformer       *TransformContractDataStruct
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
	contractInstanceData, ok := contractData.Val.GetInstance()
	if !ok || contractInstanceData.Storage == nil {
		return nil
	}

	nativeAssetContractID, err := xdr.MustNewNativeAsset().ContractID(passphrase)
	if err != nil {
		return nil
	}

	var assetInfo *xdr.ScVal
	for _, mapEntry := range *contractInstanceData.Storage {
		if mapEntry.Key.Equals(assetInfoKey) {
			// clone the map entry to avoid reference to loop iterator
			mapValXdr, cloneErr := mapEntry.Val.MarshalBinary()
			if cloneErr != nil {
				return nil
			}
			assetInfo = &xdr.ScVal{}
			cloneErr = assetInfo.UnmarshalBinary(mapValXdr)
			if cloneErr != nil {
				return nil
			}
			break
		}
	}

	if assetInfo == nil {
		return nil
	}

	vecPtr, ok := assetInfo.GetVec()
	if !ok || vecPtr == nil || len(*vecPtr) != 2 {
		return nil
	}
	vec := *vecPtr

	sym, ok := vec[0].GetSym()
	if !ok {
		return nil
	}
	switch sym {
	case "AlphaNum4":
	case "AlphaNum12":
	case "Native":
		if contractData.Contract.ContractId != nil && (*contractData.Contract.ContractId) == nativeAssetContractID {
			asset := xdr.MustNewNativeAsset()
			return &asset
		}
	default:
		return nil
	}

	var assetCode, assetIssuer string
	assetMapPtr, ok := vec[1].GetMap()
	if !ok || assetMapPtr == nil || len(*assetMapPtr) != 2 {
		return nil
	}
	assetMap := *assetMapPtr

	assetCodeEntry, assetIssuerEntry := assetMap[0], assetMap[1]
	if sym, ok = assetCodeEntry.Key.GetSym(); !ok || sym != assetCodeSym {
		return nil
	}
	assetCodeSc, ok := assetCodeEntry.Val.GetStr()
	if !ok {
		return nil
	}
	if assetCode = string(assetCodeSc); assetCode == "" {
		return nil
	}

	if sym, ok = assetIssuerEntry.Key.GetSym(); !ok || sym != issuerSym {
		return nil
	}
	assetIssuerSc, ok := assetIssuerEntry.Val.GetBytes()
	if !ok {
		return nil
	}
	assetIssuer, err = strkey.Encode(strkey.VersionByteAccountID, assetIssuerSc)
	if err != nil {
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
	if contractData.Contract.ContractId == nil || expectedID != *(contractData.Contract.ContractId) {
		return nil
	}

	return &asset
}

// Helper functions for balance extraction
func ContractBalanceFromContractData(ledgerEntry xdr.LedgerEntry, passphrase string) (*xdr.ScAddress, *big.Int, bool) {
	contractData, ok := ledgerEntry.Data.GetContractData()
	if !ok {
		return nil, nil, false
	}

	_, err := xdr.MustNewNativeAsset().ContractID(passphrase)
	if err != nil {
		return nil, nil, false
	}

	if contractData.Contract.ContractId == nil {
		return nil, nil, false
	}

	keyEnumVecPtr, ok := contractData.Key.GetVec()
	if !ok || keyEnumVecPtr == nil {
		return nil, nil, false
	}
	keyEnumVec := *keyEnumVecPtr
	if len(keyEnumVec) != 2 || !keyEnumVec[0].Equals(
		xdr.ScVal{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &balanceMetadataSym,
		},
	) {
		return nil, nil, false
	}

	scAddress, ok := keyEnumVec[1].GetAddress()
	if !ok {
		return nil, nil, false
	}

	balanceMapPtr, ok := contractData.Val.GetMap()
	if !ok || balanceMapPtr == nil {
		return nil, nil, false
	}
	balanceMap := *balanceMapPtr
	if !ok || len(balanceMap) != 3 {
		return nil, nil, false
	}

	var keySym xdr.ScSymbol
	if keySym, ok = balanceMap[0].Key.GetSym(); !ok || keySym != "amount" {
		return nil, nil, false
	}
	if keySym, ok = balanceMap[1].Key.GetSym(); !ok || keySym != "authorized" ||
		!balanceMap[1].Val.IsBool() {
		return nil, nil, false
	}
	if keySym, ok = balanceMap[2].Key.GetSym(); !ok || keySym != "clawback" ||
		!balanceMap[2].Val.IsBool() {
		return nil, nil, false
	}
	amount, ok := balanceMap[0].Val.GetI128()
	if !ok {
		return nil, nil, false
	}

	// amount cannot be negative
	if int64(amount.Hi) < 0 {
		return nil, nil, false
	}
	amt := new(big.Int).Lsh(new(big.Int).SetInt64(int64(amount.Hi)), 64)
	amt.Add(amt, new(big.Int).SetUint64(uint64(amount.Lo)))
	return &scAddress, amt, true
}

func NewContractLedgerReader(config map[string]interface{}) (*ContractLedgerReader, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	transformer := NewTransformContractDataStruct(AssetFromContractData, ContractBalanceFromContractData)

	return &ContractLedgerReader{
		networkPassphrase: networkPassphrase,
		transformer:       transformer,
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

		log.Printf("Processing transaction: %s", tx.Result.TransactionHash.HexString())

		// Process each operation's changes
		if tx.UnsafeMeta.V1 == nil {
			continue
		}

		for i := range tx.UnsafeMeta.V1.Operations {
			changes := tx.UnsafeMeta.V1.Operations[i].Changes
			log.Printf("Found %d changes in operation %d", len(changes), i)

			for _, change := range changes {
				output, err, ok := p.transformer.TransformContractData(change, p.networkPassphrase, ledgerCloseMeta.LedgerHeaderHistoryEntry())
				if err != nil {
					log.Printf("Error transforming contract data: %v", err)
					continue
				}
				if !ok {
					continue
				}

				// Forward the transformed data
				if err := p.forwardToProcessors(ctx, output); err != nil {
					log.Printf("Error forwarding contract data: %v", err)
					continue
				}

				p.mu.Lock()
				p.stats.ProcessedChanges++
				p.mu.Unlock()
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

func (p *ContractLedgerReader) forwardToProcessors(ctx context.Context, data ContractDataOutput) error {
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

// Add these type definitions after the existing var block
type AssetFromContractDataFunc func(ledgerEntry xdr.LedgerEntry, passphrase string) *xdr.Asset
type ContractBalanceFromContractDataFunc func(ledgerEntry xdr.LedgerEntry, passphrase string) (*xdr.ScAddress, *big.Int, bool)

type TransformContractDataStruct struct {
	AssetFromContractData           AssetFromContractDataFunc
	ContractBalanceFromContractData ContractBalanceFromContractDataFunc
}

type ContractDataOutput struct {
	ContractId                string    `json:"contract_id"`
	ContractKeyType           string    `json:"contract_key_type"`
	ContractDurability        string    `json:"contract_durability"`
	ContractDataAssetCode     string    `json:"asset_code"`
	ContractDataAssetIssuer   string    `json:"asset_issuer"`
	ContractDataAssetType     string    `json:"asset_type"`
	ContractDataBalanceHolder string    `json:"balance_holder"`
	ContractDataBalance       string    `json:"balance"` // balance is a string because it is go type big.Int
	LastModifiedLedger        uint32    `json:"last_modified_ledger"`
	LedgerEntryChange         uint32    `json:"ledger_entry_change"`
	Deleted                   bool      `json:"deleted"`
	ClosedAt                  time.Time `json:"closed_at"`
	LedgerSequence            uint32    `json:"ledger_sequence"`
	LedgerKeyHash             string    `json:"ledger_key_hash"`
}

// Add this function after the existing helper functions
func NewTransformContractDataStruct(assetFrom AssetFromContractDataFunc, contractBalance ContractBalanceFromContractDataFunc) *TransformContractDataStruct {
	return &TransformContractDataStruct{
		AssetFromContractData:           assetFrom,
		ContractBalanceFromContractData: contractBalance,
	}
}

// Add this method to TransformContractDataStruct
func (t *TransformContractDataStruct) TransformContractData(change xdr.LedgerEntryChange, passphrase string, header xdr.LedgerHeaderHistoryEntry) (ContractDataOutput, error, bool) {
	ledgerEntry, changeType, outputDeleted, err := ExtractEntryFromXDRChange(change)
	if err != nil {
		return ContractDataOutput{}, err, false
	}

	contractData, ok := ledgerEntry.Data.GetContractData()
	if !ok {
		return ContractDataOutput{}, fmt.Errorf("could not extract contract data from ledger entry; actual type is %s", ledgerEntry.Data.Type), false
	}

	if contractData.Key.Type.String() == "ScValTypeScvLedgerKeyNonce" {
		// Is a nonce and should be discarded
		return ContractDataOutput{}, nil, false
	}

	ledgerKeyHash, err := LedgerEntryToLedgerKeyHash(ledgerEntry)
	if err != nil {
		return ContractDataOutput{}, err, false
	}

	var contractDataAssetType string
	var contractDataAssetCode string
	var contractDataAssetIssuer string

	contractDataAsset := t.AssetFromContractData(ledgerEntry, passphrase)
	if contractDataAsset != nil {
		contractDataAssetType = contractDataAsset.Type.String()
		contractDataAssetCode = contractDataAsset.GetCode()
		contractDataAssetCode = strings.ReplaceAll(contractDataAssetCode, "\x00", "")
		contractDataAssetIssuer = contractDataAsset.GetIssuer()
	}

	var contractDataBalanceHolder string
	var contractDataBalance string

	dataBalanceHolder, dataBalance, _ := t.ContractBalanceFromContractData(ledgerEntry, passphrase)
	if dataBalance != nil && dataBalanceHolder != nil {
		// Properly encode the balance holder address based on its type
		switch dataBalanceHolder.Type {
		case xdr.ScAddressTypeScAddressTypeAccount:
			if dataBalanceHolder.AccountId != nil {
				accountID := dataBalanceHolder.AccountId.Ed25519
				contractDataBalanceHolder, _ = strkey.Encode(strkey.VersionByteAccountID, accountID[:])
			}
		case xdr.ScAddressTypeScAddressTypeContract:
			if dataBalanceHolder.ContractId != nil {
				contractID := *dataBalanceHolder.ContractId
				contractDataBalanceHolder, _ = strkey.Encode(strkey.VersionByteContract, contractID[:])
			}
		case xdr.ScAddressTypeScAddressTypeMuxedAccount:
			if dataBalanceHolder.MuxedAccount != nil {
				// For muxed accounts, encode with Ed25519 + ID
				muxedData := make([]byte, 40) // 32 bytes for Ed25519 + 8 bytes for ID
				copy(muxedData[:32], dataBalanceHolder.MuxedAccount.Ed25519[:])
				// Encode the ID as big-endian uint64
				for i := 0; i < 8; i++ {
					muxedData[32+i] = byte(dataBalanceHolder.MuxedAccount.Id >> (56 - 8*i))
				}
				contractDataBalanceHolder, _ = strkey.Encode(strkey.VersionByteMuxedAccount, muxedData)
			}
		case xdr.ScAddressTypeScAddressTypeClaimableBalance:
			if dataBalanceHolder.ClaimableBalanceId != nil {
				// ClaimableBalanceId is a union, currently only V0 is supported
				var claimableBalanceHash [32]byte
				switch dataBalanceHolder.ClaimableBalanceId.Type {
				case xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0:
					claimableBalanceHash = [32]byte(*dataBalanceHolder.ClaimableBalanceId.V0)
				}
				contractDataBalanceHolder, _ = strkey.Encode(strkey.VersionByteClaimableBalance, claimableBalanceHash[:])
			}
		case xdr.ScAddressTypeScAddressTypeLiquidityPool:
			if dataBalanceHolder.LiquidityPoolId != nil {
				poolID := [32]byte(*dataBalanceHolder.LiquidityPoolId)
				contractDataBalanceHolder, _ = strkey.Encode(strkey.VersionByteLiquidityPool, poolID[:])
			}
		}
		contractDataBalance = dataBalance.String()
	}

	contractDataContractId, ok := contractData.Contract.GetContractId()
	if !ok {
		return ContractDataOutput{}, fmt.Errorf("could not extract contractId data information from contractData"), false
	}

	contractDataKeyType := contractData.Key.Type.String()
	contractDataContractIdByte, _ := contractDataContractId.MarshalBinary()
	outputContractDataContractId, _ := strkey.Encode(strkey.VersionByteContract, contractDataContractIdByte)

	contractDataDurability := contractData.Durability.String()

	closedAt := time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()

	transformedData := ContractDataOutput{
		ContractId:                outputContractDataContractId,
		ContractKeyType:           contractDataKeyType,
		ContractDurability:        contractDataDurability,
		ContractDataAssetCode:     contractDataAssetCode,
		ContractDataAssetIssuer:   contractDataAssetIssuer,
		ContractDataAssetType:     contractDataAssetType,
		ContractDataBalanceHolder: contractDataBalanceHolder,
		ContractDataBalance:       contractDataBalance,
		LastModifiedLedger:        uint32(ledgerEntry.LastModifiedLedgerSeq),
		LedgerEntryChange:         uint32(changeType),
		Deleted:                   outputDeleted,
		ClosedAt:                  closedAt,
		LedgerSequence:            uint32(header.Header.LedgerSeq),
		LedgerKeyHash:             ledgerKeyHash,
	}

	return transformedData, nil, true
}
