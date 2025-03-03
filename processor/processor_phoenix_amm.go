package processor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// PhoenixEvent represents a processed Phoenix AMM event
type PhoenixEvent struct {
	Type           string    `json:"type"`
	PoolID         string    `json:"pool_id"`
	AssetA         Asset     `json:"asset_a"`
	AssetB         Asset     `json:"asset_b"`
	ReserveA       string    `json:"reserve_a"`
	ReserveB       string    `json:"reserve_b"`
	TotalShares    string    `json:"total_shares"`
	Fee            uint32    `json:"fee"`
	Action         string    `json:"action"` // swap, deposit, withdraw
	LedgerSequence uint32    `json:"ledger_sequence"`
	TransactionID  string    `json:"transaction_id"`
	Timestamp      time.Time `json:"timestamp"`
}

// PhoenixAMMConfig holds the configuration for the Phoenix AMM processor
type PhoenixAMMConfig struct {
	NetworkPassphrase string
	ContractIDs       []string // List of valid Phoenix contract IDs for the network
	Processors        []Processor
}

// PhoenixAMMProcessor processes Phoenix AMM contract events
type PhoenixAMMProcessor struct {
	networkPassphrase string
	contractIDs       []string
	processors        []Processor
	stats             struct {
		ProcessedEvents uint64
		SwapEvents      uint64
		DepositEvents   uint64
		WithdrawEvents  uint64
		LastEventTime   time.Time
	}
	mu sync.RWMutex
}

// NewPhoenixAMMProcessor creates a new Phoenix AMM processor
func NewPhoenixAMMProcessor(config PhoenixAMMConfig) *PhoenixAMMProcessor {
	return &PhoenixAMMProcessor{
		networkPassphrase: config.NetworkPassphrase,
		contractIDs:       config.ContractIDs,
		processors:        config.Processors,
		stats: struct {
			ProcessedEvents uint64
			SwapEvents      uint64
			DepositEvents   uint64
			WithdrawEvents  uint64
			LastEventTime   time.Time
		}{
			ProcessedEvents: 0,
			SwapEvents:      0,
			DepositEvents:   0,
			WithdrawEvents:  0,
			LastEventTime:   time.Time{},
		},
	}
}

func (p *PhoenixAMMProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *PhoenixAMMProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
	}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

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
			if op.Body.Type != xdr.OperationTypeInvokeHostFunction {
				continue
			}

			// Process Phoenix AMM operations
			event, err := p.processPhoenixOperation(op, tx, ledgerCloseMeta)
			if err != nil {
				log.Printf("Error processing Phoenix operation: %v", err)
				continue
			}

			if event != nil {
				p.updateStats(event.Action)
				if err := p.forwardToProcessors(ctx, event); err != nil {
					return fmt.Errorf("error forwarding event: %w", err)
				}
			}
		}
	}

	return nil
}

func (p *PhoenixAMMProcessor) processPhoenixOperation(op xdr.Operation, tx ingest.LedgerTransaction, lcm xdr.LedgerCloseMeta) (*PhoenixEvent, error) {
	if op.Body.Type != xdr.OperationTypeInvokeHostFunction {
		return nil, nil
	}

	hostFn := op.Body.MustInvokeHostFunctionOp()
	// Check if this is a Phoenix contract call
	if !p.isPhoenixContract(hostFn.HostFunction) {
		return nil, nil
	}

	// Extract function and parameters
	fnType, params, err := parsePhoenixFunction(hostFn.HostFunction)
	if err != nil {
		return nil, fmt.Errorf("error parsing Phoenix function: %w", err)
	}

	// Get pool details from the changes
	changes, err := tx.GetOperationChanges(uint32(tx.Index))
	if err != nil {
		return nil, fmt.Errorf("error getting operation changes: %w", err)
	}

	// Build the event based on the function type
	event := &PhoenixEvent{
		Type:           "phoenix_amm",
		LedgerSequence: lcm.LedgerSequence(),
		TransactionID:  tx.Result.TransactionHash.HexString(),
		Timestamp:      time.Unix(int64(lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0),
	}

	switch fnType {
	case "swap":
		if err := p.processSwap(event, params, changes); err != nil {
			return nil, err
		}
		event.Action = "swap"
	case "deposit":
		if err := p.processDeposit(event, params, changes); err != nil {
			return nil, err
		}
		event.Action = "deposit"
	case "withdraw":
		if err := p.processWithdraw(event, params, changes); err != nil {
			return nil, err
		}
		event.Action = "withdraw"
	default:
		return nil, nil
	}

	return event, nil
}

func (p *PhoenixAMMProcessor) updateStats(action string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats.ProcessedEvents++
	p.stats.LastEventTime = time.Now()

	switch action {
	case "swap":
		p.stats.SwapEvents++
	case "deposit":
		p.stats.DepositEvents++
	case "withdraw":
		p.stats.WithdrawEvents++
	}
}

func (p *PhoenixAMMProcessor) forwardToProcessors(ctx context.Context, event *PhoenixEvent) error {
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling event: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	return nil
}

// isPhoenixContract checks if the host function is calling a Phoenix AMM contract
func (p *PhoenixAMMProcessor) isPhoenixContract(hostFn xdr.HostFunction) bool {
	if hostFn.Type != xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		return false
	}

	contractID := hostFn.MustInvokeContract().ContractAddress.ContractId
	contractIDStr := hex.EncodeToString(contractID[:])

	// Check against configured Phoenix contract IDs
	for _, knownID := range p.contractIDs {
		if contractIDStr == knownID {
			return true
		}
	}
	return false
}

// parsePhoenixFunction extracts the function type and parameters from a Phoenix contract call
func parsePhoenixFunction(hostFn xdr.HostFunction) (string, map[string]interface{}, error) {
	invokeContract := hostFn.MustInvokeContract()
	functionName := string(invokeContract.FunctionName)
	params := make(map[string]interface{})

	// Parse function parameters based on the Stellar Contract ABI
	switch functionName {
	case "swap":
		if len(invokeContract.Args) < 3 {
			return "", nil, fmt.Errorf("invalid number of arguments for swap")
		}
		params["amount_in"] = invokeContract.Args[0]
		params["min_amount_out"] = invokeContract.Args[1]
		params["direction"] = invokeContract.Args[2]

	case "deposit":
		if len(invokeContract.Args) < 4 {
			return "", nil, fmt.Errorf("invalid number of arguments for deposit")
		}
		params["max_amount_a"] = invokeContract.Args[0]
		params["max_amount_b"] = invokeContract.Args[1]
		params["min_pool_shares"] = invokeContract.Args[2]
		params["deadline"] = invokeContract.Args[3]

	case "withdraw":
		if len(invokeContract.Args) < 3 {
			return "", nil, fmt.Errorf("invalid number of arguments for withdraw")
		}
		params["pool_shares"] = invokeContract.Args[0]
		params["min_amount_a"] = invokeContract.Args[1]
		params["min_amount_b"] = invokeContract.Args[2]
	}

	return functionName, params, nil
}

// processSwap handles swap operations in the Phoenix AMM
func (p *PhoenixAMMProcessor) processSwap(event *PhoenixEvent, params map[string]interface{}, changes []ingest.Change) error {
	// Extract pool state changes
	poolState, err := findPoolState(changes)
	if err != nil {
		return fmt.Errorf("error finding pool state: %w", err)
	}

	// Extract pool details from contract data
	poolDetails, err := extractPoolDetails(poolState)
	if err != nil {
		return fmt.Errorf("error extracting pool details: %w", err)
	}

	// Update event with pool details
	event.PoolID = poolDetails.ID
	event.AssetA = poolDetails.AssetA
	event.AssetB = poolDetails.AssetB
	event.ReserveA = amount.String(poolDetails.ReserveA)
	event.ReserveB = amount.String(poolDetails.ReserveB)
	event.TotalShares = amount.String(poolDetails.TotalShares)
	event.Fee = poolDetails.Fee

	return nil
}

// processDeposit handles deposit operations in the Phoenix AMM
func (p *PhoenixAMMProcessor) processDeposit(event *PhoenixEvent, params map[string]interface{}, changes []ingest.Change) error {
	poolState, err := findPoolState(changes)
	if err != nil {
		return err
	}

	poolDetails, err := extractPoolDetails(poolState)
	if err != nil {
		return fmt.Errorf("error extracting pool details: %w", err)
	}

	event.PoolID = poolDetails.ID
	event.AssetA = poolDetails.AssetA
	event.AssetB = poolDetails.AssetB
	event.ReserveA = amount.String(poolDetails.ReserveA)
	event.ReserveB = amount.String(poolDetails.ReserveB)
	event.TotalShares = amount.String(poolDetails.TotalShares)
	event.Fee = poolDetails.Fee

	return nil
}

// processWithdraw handles withdraw operations in the Phoenix AMM
func (p *PhoenixAMMProcessor) processWithdraw(event *PhoenixEvent, params map[string]interface{}, changes []ingest.Change) error {
	poolState, err := findPoolState(changes)
	if err != nil {
		return err
	}

	poolDetails, err := extractPoolDetails(poolState)
	if err != nil {
		return fmt.Errorf("error extracting pool details: %w", err)
	}

	event.PoolID = poolDetails.ID
	event.AssetA = poolDetails.AssetA
	event.AssetB = poolDetails.AssetB
	event.ReserveA = amount.String(poolDetails.ReserveA)
	event.ReserveB = amount.String(poolDetails.ReserveB)
	event.TotalShares = amount.String(poolDetails.TotalShares)
	event.Fee = poolDetails.Fee

	return nil
}

// Helper function to find pool state in changes
func findPoolState(changes []ingest.Change) (*xdr.LedgerEntry, error) {
	for _, change := range changes {
		if change.LedgerEntryChangeType() == xdr.LedgerEntryChangeTypeLedgerEntryState {
			if entry := change.Pre; entry != nil && entry.Data.Type == xdr.LedgerEntryTypeContractData {
				return entry, nil
			}
		}
	}
	return nil, fmt.Errorf("pool state not found in changes")
}

// PoolState represents the Phoenix AMM pool state
type PoolState struct {
	ID           string // Pool identifier
	Version      uint32
	AssetA       Asset
	AssetB       Asset
	ReserveA     xdr.Int64
	ReserveB     xdr.Int64
	TotalShares  xdr.Int64
	Fee          uint32
	LastModified uint64
}

// extractPoolDetails parses the contract data to extract pool details
func extractPoolDetails(contractData *xdr.LedgerEntry) (*PoolState, error) {
	if contractData == nil {
		return nil, fmt.Errorf("contract data is nil")
	}

	var pool PoolState
	data, ok := contractData.Data.GetContractData()
	if !ok {
		return nil, fmt.Errorf("failed to get contract data")
	}

	key := data.Key
	val := data.Val

	if key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
		return nil, fmt.Errorf("unexpected contract data key type: %v", key.Type)
	}

	// Parse contract data value
	if val.Type != xdr.ScValTypeScvVec {
		return nil, fmt.Errorf("unexpected contract data value type: %v", val.Type)
	}

	vec, ok := val.GetVec()
	if !ok || vec == nil {
		return nil, fmt.Errorf("failed to get vector data")
	}

	values := *vec // Dereference the vector pointer to get the slice
	if len(values) < 7 {
		return nil, fmt.Errorf("invalid pool data length: got %d, want at least 7", len(values))
	}

	// Extract version
	version, ok := values[0].GetU32()
	if !ok {
		return nil, fmt.Errorf("invalid version format")
	}
	pool.Version = uint32(version)

	// Extract assets
	if err := extractAssetData(&pool.AssetA, &values[1]); err != nil {
		return nil, fmt.Errorf("error extracting asset A: %w", err)
	}
	if err := extractAssetData(&pool.AssetB, &values[2]); err != nil {
		return nil, fmt.Errorf("error extracting asset B: %w", err)
	}

	// Extract reserves
	reserveA, ok := values[3].GetI64()
	if !ok {
		return nil, fmt.Errorf("invalid reserve A format")
	}
	pool.ReserveA = reserveA

	reserveB, ok := values[4].GetI64()
	if !ok {
		return nil, fmt.Errorf("invalid reserve B format")
	}
	pool.ReserveB = reserveB

	// Extract total shares
	shares, ok := values[5].GetI64()
	if !ok {
		return nil, fmt.Errorf("invalid total shares format")
	}
	pool.TotalShares = shares

	// Extract fee
	fee, ok := values[6].GetU32()
	if !ok {
		return nil, fmt.Errorf("invalid fee format")
	}
	pool.Fee = uint32(fee)

	// Set pool ID from contract ID
	contractID := data.Contract.ContractId
	pool.ID = hex.EncodeToString(contractID[:])

	return &pool, nil
}

// extractAssetData parses asset data from ScVal
func extractAssetData(asset *Asset, val *xdr.ScVal) error {
	vec, ok := val.GetVec()
	if !ok || vec == nil {
		return fmt.Errorf("failed to get asset vector data")
	}

	values := *vec // Dereference the vector pointer to get the slice
	if len(values) < 2 {
		return fmt.Errorf("invalid asset data length: got %d, want at least 2", len(values))
	}

	// Extract asset type
	assetType, ok := values[0].GetStr()
	if !ok {
		return fmt.Errorf("invalid asset type format")
	}
	asset.Type = string(assetType)

	// Extract asset code and issuer based on type
	switch asset.Type {
	case "native":
		asset.Code = "XLM"
	case "credit_alphanum4", "credit_alphanum12":
		code, ok := values[1].GetStr()
		if !ok {
			return fmt.Errorf("invalid asset code format")
		}
		asset.Code = string(code)

		if len(values) < 3 {
			return fmt.Errorf("missing issuer for non-native asset")
		}
		issuer, ok := values[2].GetStr()
		if !ok {
			return fmt.Errorf("invalid issuer format")
		}
		asset.Issuer = string(issuer)
	default:
		return fmt.Errorf("unknown asset type: %s", asset.Type)
	}

	return nil
}

// validatePoolStateChange ensures the pool state change is valid
func validatePoolStateChange(oldState, newState *PoolState) error {
	if oldState == nil || newState == nil {
		return fmt.Errorf("state cannot be nil")
	}

	// Validate asset consistency
	if oldState.AssetA != newState.AssetA || oldState.AssetB != newState.AssetB {
		return fmt.Errorf("pool assets cannot change")
	}

	// Validate reserves
	if newState.ReserveA < 0 || newState.ReserveB < 0 {
		return fmt.Errorf("reserves cannot be negative")
	}

	// Validate total shares
	if newState.TotalShares < 0 {
		return fmt.Errorf("total shares cannot be negative")
	}

	// Validate fee
	if newState.Fee > 10000 { // Max fee is 100% (10000 basis points)
		return fmt.Errorf("invalid fee: %d", newState.Fee)
	}

	return nil
}
