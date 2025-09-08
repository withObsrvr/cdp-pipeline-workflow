package processor

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

// WalletBackendStateChangeType represents the type of state change
type WalletBackendStateChangeType string

const (
	// Token movements
	StateChangeTypeCredit    WalletBackendStateChangeType = "CREDIT"
	StateChangeTypeDebit     WalletBackendStateChangeType = "DEBIT"
	StateChangeTypeMint      WalletBackendStateChangeType = "MINT"
	StateChangeTypeBurn      WalletBackendStateChangeType = "BURN"
	StateChangeTypeClawback  WalletBackendStateChangeType = "CLAWBACK"
	
	// Account configuration
	StateChangeTypeSigner    WalletBackendStateChangeType = "SIGNER"
	StateChangeTypeThreshold WalletBackendStateChangeType = "THRESHOLD"
	StateChangeTypeFlags     WalletBackendStateChangeType = "FLAGS"
	StateChangeTypeInflation WalletBackendStateChangeType = "INFLATION"
	
	// Metadata
	StateChangeTypeHomeDomain WalletBackendStateChangeType = "HOME_DOMAIN"
	StateChangeTypeDataEntry  WalletBackendStateChangeType = "DATA_ENTRY"
	
	// Sponsorship
	StateChangeTypeSponsorship WalletBackendStateChangeType = "SPONSORSHIP"
	
	// Contract
	StateChangeTypeContractData  WalletBackendStateChangeType = "CONTRACT_DATA"
	StateChangeTypeContractEvent WalletBackendStateChangeType = "CONTRACT_EVENT"
	
	// Account lifecycle
	StateChangeTypeAccountCreated WalletBackendStateChangeType = "ACCOUNT_CREATED"
	StateChangeTypeAccountRemoved WalletBackendStateChangeType = "ACCOUNT_REMOVED"
	
	// Trustlines
	StateChangeTypeTrustlineCreated WalletBackendStateChangeType = "TRUSTLINE_CREATED"
	StateChangeTypeTrustlineUpdated WalletBackendStateChangeType = "TRUSTLINE_UPDATED"
	StateChangeTypeTrustlineRemoved WalletBackendStateChangeType = "TRUSTLINE_REMOVED"
	
	// Offers
	StateChangeTypeOfferCreated WalletBackendStateChangeType = "OFFER_CREATED"
	StateChangeTypeOfferUpdated WalletBackendStateChangeType = "OFFER_UPDATED"
	StateChangeTypeOfferRemoved WalletBackendStateChangeType = "OFFER_REMOVED"
	
	// Liquidity pools
	StateChangeTypeLiquidityPoolDeposit  WalletBackendStateChangeType = "LIQUIDITY_POOL_DEPOSIT"
	StateChangeTypeLiquidityPoolWithdraw WalletBackendStateChangeType = "LIQUIDITY_POOL_WITHDRAW"
)

// WalletBackendStateChange represents a single state change in the blockchain
type WalletBackendStateChange struct {
	// Core identifiers
	LedgerSequence    uint32          `json:"ledger_sequence"`
	TransactionHash   string          `json:"transaction_hash"`
	TransactionIndex  int32           `json:"transaction_index"`
	OperationIndex    int32           `json:"operation_index"`
	OperationType     xdr.OperationType `json:"operation_type"`
	ApplicationOrder  int32           `json:"application_order"`
	
	// State change details
	Type              WalletBackendStateChangeType `json:"type"`
	Account           string          `json:"account"`
	Timestamp         int64           `json:"timestamp"`
	
	// Optional fields based on type
	Asset             *Asset          `json:"asset,omitempty"`
	Amount            *string         `json:"amount,omitempty"`
	BalanceBefore     *string         `json:"balance_before,omitempty"`
	BalanceAfter      *string         `json:"balance_after,omitempty"`
	
	// For account configuration changes
	SignerKey         *string         `json:"signer_key,omitempty"`
	SignerWeight      *uint32         `json:"signer_weight,omitempty"`
	ThresholdType     *string         `json:"threshold_type,omitempty"`
	ThresholdValue    *uint32         `json:"threshold_value,omitempty"`
	FlagsSet          *uint32         `json:"flags_set,omitempty"`
	FlagsClear        *uint32         `json:"flags_clear,omitempty"`
	
	// For data entries
	DataName          *string         `json:"data_name,omitempty"`
	DataValue         *string         `json:"data_value,omitempty"`
	
	// For sponsorship
	SponsorshipAccount *string        `json:"sponsorship_account,omitempty"`
	
	// For contract events
	ContractID        *string         `json:"contract_id,omitempty"`
	ContractEventType *string         `json:"contract_event_type,omitempty"`
	ContractEventData interface{}     `json:"contract_event_data,omitempty"`
	
	// Metadata
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
}

// WalletBackendOutput is the message format emitted by the processor
type WalletBackendOutput struct {
	LedgerSequence uint32                     `json:"ledger_sequence"`
	Changes        []WalletBackendStateChange `json:"changes"`
	Participants   []string                   `json:"participants"` // All accounts involved
}

// WalletBackendProcessor extracts all state changes from ledger transactions
type WalletBackendProcessor struct {
	shouldExtractContractEvents bool
	trackParticipants          bool
	subscribers                []Processor
	networkPassphrase          string
}

// ProcessorWalletBackend creates a new wallet backend processor
func ProcessorWalletBackend(config map[string]interface{}) Processor {
	extractContractEvents := true
	trackParticipants := true
	networkPassphrase := "Test SDF Network ; September 2015" // Default to testnet
	
	// Parse configuration
	if val, ok := config["extract_contract_events"].(bool); ok {
		extractContractEvents = val
	}
	if val, ok := config["track_participants"].(bool); ok {
		trackParticipants = val
	}
	if val, ok := config["network_passphrase"].(string); ok {
		networkPassphrase = val
	}
	
	return &WalletBackendProcessor{
		shouldExtractContractEvents: extractContractEvents,
		trackParticipants:          trackParticipants,
		subscribers:                []Processor{},
		networkPassphrase:          networkPassphrase,
	}
}

// Subscribe sets the next processor in the chain
func (p *WalletBackendProcessor) Subscribe(proc Processor) {
	p.subscribers = append(p.subscribers, proc)
}

// Process extracts state changes from ledger transactions
func (p *WalletBackendProcessor) Process(ctx context.Context, msg Message) error {
	// Extract LedgerCloseMeta
	ledgerCloseMeta, ok := msg.Payload.(*xdr.LedgerCloseMeta)
	if !ok {
		// Check if it's already a pass-through message
		if lcm, ok := msg.Payload.(xdr.LedgerCloseMeta); ok {
			ledgerCloseMeta = &lcm
		} else {
			return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
		}
	}

	// Process all transactions in the ledger
	outputs, err := p.processLedger(ctx, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("failed to process ledger: %w", err)
	}

	// Create new message with original payload and updated metadata
	outputMsg := Message{
		Payload:  ledgerCloseMeta,
		Metadata: msg.Metadata,
	}

	// Initialize metadata if nil
	if outputMsg.Metadata == nil {
		outputMsg.Metadata = make(map[string]interface{})
	}

	// Add state changes to metadata
	outputMsg.Metadata["state_changes"] = outputs
	outputMsg.Metadata["processor_wallet_backend"] = true

	// Forward to subscribers
	for _, subscriber := range p.subscribers {
		if err := subscriber.Process(ctx, outputMsg); err != nil {
			log.Errorf("Error in subscriber processing: %v", err)
		}
	}

	return nil
}

// processLedger processes all transactions in a ledger
func (p *WalletBackendProcessor) processLedger(ctx context.Context, ledgerCloseMeta *xdr.LedgerCloseMeta) ([]WalletBackendOutput, error) {
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, *ledgerCloseMeta)
	if err != nil {
		return nil, fmt.Errorf("error creating ledger transaction reader: %w", err)
	}
	defer ledgerTxReader.Close()

	var outputs []WalletBackendOutput

	// Process each transaction in the ledger
	for {
		tx, err := ledgerTxReader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Errorf("Error reading transaction: %v", err)
			continue
		}

		if !tx.Result.Successful() {
			continue // Skip failed transactions
		}

		output := &WalletBackendOutput{
			LedgerSequence: uint32(ledgerCloseMeta.LedgerSequence()),
			Changes:        []WalletBackendStateChange{},
			Participants:   []string{},
		}
		
		participantSet := make(map[string]bool)

		// Process each operation in the transaction
		for opIndex, op := range tx.Envelope.Operations() {
			changes, participants := p.processOperation(&tx, op, opIndex)
			output.Changes = append(output.Changes, changes...)
			
			// Track participants
			for _, participant := range participants {
				participantSet[participant] = true
			}
		}
		
		// Extract changes from transaction metadata
		metaChanges := p.extractMetadataChanges(&tx)
		output.Changes = append(output.Changes, metaChanges...)
		
		// Process contract events if enabled
		if p.shouldExtractContractEvents {
			contractChanges, err := p.extractContractEvents(&tx)
			if err == nil {
				output.Changes = append(output.Changes, contractChanges...)
			}
		}
		
		// Convert participant set to slice
		if p.trackParticipants {
			for participant := range participantSet {
				output.Participants = append(output.Participants, participant)
			}
		}
		
		// Sort changes by application order
		p.sortChangesByOrder(output.Changes)
		
		outputs = append(outputs, *output)
	}

	return outputs, nil
}

// processOperation extracts state changes from a single operation
func (p *WalletBackendProcessor) processOperation(tx *ingest.LedgerTransaction, op xdr.Operation, opIndex int) ([]WalletBackendStateChange, []string) {
	changes := []WalletBackendStateChange{}
	participants := []string{}
	
	baseChange := WalletBackendStateChange{
		LedgerSequence:   uint32(tx.Ledger.LedgerSequence()),
		TransactionHash:  tx.Result.TransactionHash.HexString(),
		TransactionIndex: int32(tx.Index),
		OperationIndex:   int32(opIndex),
		OperationType:    op.Body.Type,
		Timestamp:        time.Now().Unix(), // Use current time as approximation
	}
	
	// Add source account as participant
	sourceAccount := op.SourceAccount
	if sourceAccount == nil {
		txSource := tx.Envelope.SourceAccount()
		sourceAccount = &txSource
	}
	sourceAddr := sourceAccount.ToAccountId().Address()
	participants = append(participants, sourceAddr)
	
	switch op.Body.Type {
	case xdr.OperationTypePayment:
		payment, ok := op.Body.GetPaymentOp()
		if !ok {
			break
		}
		destAddr := payment.Destination.ToAccountId().Address()
		participants = append(participants, destAddr)
		
		// Create debit change for source
		debitChange := baseChange
		debitChange.Type = StateChangeTypeDebit
		debitChange.Account = sourceAddr
		debitChange.Asset = p.xdrAssetToAsset(payment.Asset)
		amount := strconv.FormatInt(int64(payment.Amount), 10)
		debitChange.Amount = &amount
		changes = append(changes, debitChange)
		
		// Create credit change for destination
		creditChange := baseChange
		creditChange.Type = StateChangeTypeCredit
		creditChange.Account = destAddr
		creditChange.Asset = p.xdrAssetToAsset(payment.Asset)
		creditChange.Amount = &amount
		changes = append(changes, creditChange)
		
	case xdr.OperationTypeCreateAccount:
		create, ok := op.Body.GetCreateAccountOp()
		if !ok {
			break
		}
		destAddr := create.Destination.Address()
		participants = append(participants, destAddr)
		
		// Account creation change
		createChange := baseChange
		createChange.Type = StateChangeTypeAccountCreated
		createChange.Account = destAddr
		changes = append(changes, createChange)
		
		// Initial balance credit
		creditChange := baseChange
		creditChange.Type = StateChangeTypeCredit
		creditChange.Account = destAddr
		creditChange.Asset = &Asset{Code: "XLM", Type: "native"}
		amount := strconv.FormatInt(int64(create.StartingBalance), 10)
		creditChange.Amount = &amount
		changes = append(changes, creditChange)
		
	case xdr.OperationTypeSetOptions:
		setOpts, ok := op.Body.GetSetOptionsOp()
		if !ok {
			break
		}
		
		// Signer changes
		if setOpts.Signer != nil {
			signerChange := baseChange
			signerChange.Type = StateChangeTypeSigner
			signerChange.Account = sourceAddr
			
			var signerKey string
			if ed25519, ok := setOpts.Signer.Key.GetEd25519(); ok {
				signerKey = strkey.MustEncode(strkey.VersionByteAccountID, ed25519[:])
			}
			signerChange.SignerKey = &signerKey
			weight := uint32(setOpts.Signer.Weight)
			signerChange.SignerWeight = &weight
			changes = append(changes, signerChange)
		}
		
		// Threshold changes
		if setOpts.LowThreshold != nil {
			thresholdChange := baseChange
			thresholdChange.Type = StateChangeTypeThreshold
			thresholdChange.Account = sourceAddr
			thresholdType := "low"
			thresholdChange.ThresholdType = &thresholdType
			val := uint32(*setOpts.LowThreshold)
			thresholdChange.ThresholdValue = &val
			changes = append(changes, thresholdChange)
		}
		
		if setOpts.MedThreshold != nil {
			thresholdChange := baseChange
			thresholdChange.Type = StateChangeTypeThreshold
			thresholdChange.Account = sourceAddr
			thresholdType := "medium"
			thresholdChange.ThresholdType = &thresholdType
			val := uint32(*setOpts.MedThreshold)
			thresholdChange.ThresholdValue = &val
			changes = append(changes, thresholdChange)
		}
		
		if setOpts.HighThreshold != nil {
			thresholdChange := baseChange
			thresholdChange.Type = StateChangeTypeThreshold
			thresholdChange.Account = sourceAddr
			thresholdType := "high"
			thresholdChange.ThresholdType = &thresholdType
			val := uint32(*setOpts.HighThreshold)
			thresholdChange.ThresholdValue = &val
			changes = append(changes, thresholdChange)
		}
		
		// Home domain changes
		if setOpts.HomeDomain != nil {
			domainChange := baseChange
			domainChange.Type = StateChangeTypeHomeDomain
			domainChange.Account = sourceAddr
			domain := string(*setOpts.HomeDomain)
			domainChange.DataValue = &domain
			changes = append(changes, domainChange)
		}
		
		// Flag changes
		if setOpts.SetFlags != nil || setOpts.ClearFlags != nil {
			flagChange := baseChange
			flagChange.Type = StateChangeTypeFlags
			flagChange.Account = sourceAddr
			if setOpts.SetFlags != nil {
				flags := uint32(*setOpts.SetFlags)
				flagChange.FlagsSet = &flags
			}
			if setOpts.ClearFlags != nil {
				flags := uint32(*setOpts.ClearFlags)
				flagChange.FlagsClear = &flags
			}
			changes = append(changes, flagChange)
		}
		
	case xdr.OperationTypeManageData:
		manageData, ok := op.Body.GetManageDataOp()
		if !ok {
			break
		}
		
		dataChange := baseChange
		dataChange.Type = StateChangeTypeDataEntry
		dataChange.Account = sourceAddr
		dataName := string(manageData.DataName)
		dataChange.DataName = &dataName
		
		if manageData.DataValue != nil {
			dataValue := base64.StdEncoding.EncodeToString(*manageData.DataValue)
			dataChange.DataValue = &dataValue
		}
		changes = append(changes, dataChange)
		
	case xdr.OperationTypeChangeTrust:
		changeTrust, ok := op.Body.GetChangeTrustOp()
		if !ok {
			break
		}
		
		trustChange := baseChange
		trustChange.Account = sourceAddr
		trustChange.Asset = p.xdrChangeTrustAssetToAsset(changeTrust.Line)
		
		if changeTrust.Limit == 0 {
			trustChange.Type = StateChangeTypeTrustlineRemoved
		} else {
			trustChange.Type = StateChangeTypeTrustlineUpdated
			limit := strconv.FormatInt(int64(changeTrust.Limit), 10)
			trustChange.Amount = &limit
		}
		changes = append(changes, trustChange)
		
	case xdr.OperationTypeManageSellOffer, xdr.OperationTypeManageBuyOffer:
		// Extract offer changes
		offerChanges := p.extractOfferChanges(baseChange, op)
		changes = append(changes, offerChanges...)
		
	case xdr.OperationTypePathPaymentStrictReceive, xdr.OperationTypePathPaymentStrictSend:
		// Extract path payment changes
		pathChanges, pathParticipants := p.extractPathPaymentChanges(baseChange, op)
		changes = append(changes, pathChanges...)
		participants = append(participants, pathParticipants...)
	}
	
	// Set application order for all changes
	for i := range changes {
		changes[i].ApplicationOrder = int32(i)
	}
	
	return changes, participants
}

// extractMetadataChanges extracts state changes from transaction metadata
func (p *WalletBackendProcessor) extractMetadataChanges(tx *ingest.LedgerTransaction) []WalletBackendStateChange {
	stateChanges := []WalletBackendStateChange{}
	
	// Get all changes from transaction metadata
	metaChanges, err := tx.GetChanges()
	if err != nil {
		return stateChanges
	}
	
	for _, change := range metaChanges {
		// Process each ledger entry change
		switch change.Type {
		case xdr.LedgerEntryTypeAccount:
			// Account balance changes are already captured in operations
			continue
		case xdr.LedgerEntryTypeTrustline:
			// Trustline changes are already captured in operations
			continue
		case xdr.LedgerEntryTypeOffer:
			// Offer changes need special handling for trades
			continue
		}
	}
	
	return stateChanges
}

// extractContractEvents extracts state changes from Soroban contract events
func (p *WalletBackendProcessor) extractContractEvents(tx *ingest.LedgerTransaction) ([]WalletBackendStateChange, error) {
	changes := []WalletBackendStateChange{}
	
	// Use SDK helper to get transaction events
	txEvents, err := tx.GetTransactionEvents()
	if err != nil {
		return changes, err // Not a Soroban transaction
	}
	
	// Process transaction-level events
	for eventIndex, event := range txEvents.TransactionEvents {
		// Transaction events are already filtered to contract events
		_ = event // Use event variable
		
		change := WalletBackendStateChange{
			LedgerSequence:   uint32(tx.Ledger.LedgerSequence()),
			TransactionHash:  tx.Result.TransactionHash.HexString(),
			TransactionIndex: int32(tx.Index),
			OperationIndex:   0, // Contract events don't have operation index
			Type:             StateChangeTypeContractEvent,
			Timestamp:        time.Now().Unix(), // Use current time as approximation
			ApplicationOrder: int32(1000 + eventIndex), // Ensure events come after operations
		}
		
		// Note: Transaction events don't have contract ID in the same way
		// We'd need to extract it from the event data
		
		// Store event data  
		change.ContractEventData = event
		
		changes = append(changes, change)
	}
	
	// Also process operation-level events
	for opIndex, opEvents := range txEvents.OperationEvents {
		for eventIndex, event := range opEvents {
			change := WalletBackendStateChange{
				LedgerSequence:   uint32(tx.Ledger.LedgerSequence()),
				TransactionHash:  tx.Result.TransactionHash.HexString(),
				TransactionIndex: int32(tx.Index),
				OperationIndex:   int32(opIndex),
				Type:             StateChangeTypeContractEvent,
				Timestamp:        time.Now().Unix(),
				ApplicationOrder: int32(2000 + opIndex*100 + eventIndex),
			}
			
			// Extract contract ID if available
			if event.ContractId != nil {
				contractID := strkey.MustEncode(strkey.VersionByteContract, (*event.ContractId)[:])
				change.ContractID = &contractID
			}
			
			change.ContractEventData = event
			changes = append(changes, change)
		}
	}
	
	return changes, nil
}

// Helper functions

func (p *WalletBackendProcessor) xdrAssetToAsset(xdrAsset xdr.Asset) *Asset {
	switch xdrAsset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return &Asset{Code: "XLM", Type: "native"}
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a := xdrAsset.MustAlphaNum4()
		code := string(a.AssetCode[:])
		issuer, _ := strkey.Encode(strkey.VersionByteAccountID, a.Issuer.Ed25519[:])
		return &Asset{Code: code, Issuer: issuer, Type: "credit_alphanum4"}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a := xdrAsset.MustAlphaNum12()
		code := string(a.AssetCode[:])
		issuer, _ := strkey.Encode(strkey.VersionByteAccountID, a.Issuer.Ed25519[:])
		return &Asset{Code: code, Issuer: issuer, Type: "credit_alphanum12"}
	}
	return nil
}

func (p *WalletBackendProcessor) xdrChangeTrustAssetToAsset(line xdr.ChangeTrustAsset) *Asset {
	switch line.Type {
	case xdr.AssetTypeAssetTypeNative:
		return &Asset{Code: "XLM", Type: "native"}
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a := line.MustAlphaNum4()
		code := string(a.AssetCode[:])
		issuer, _ := strkey.Encode(strkey.VersionByteAccountID, a.Issuer.Ed25519[:])
		return &Asset{Code: code, Issuer: issuer, Type: "credit_alphanum4"}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a := line.MustAlphaNum12()
		code := string(a.AssetCode[:])
		issuer, _ := strkey.Encode(strkey.VersionByteAccountID, a.Issuer.Ed25519[:])
		return &Asset{Code: code, Issuer: issuer, Type: "credit_alphanum12"}
	}
	return nil
}

func (p *WalletBackendProcessor) extractOfferChanges(base WalletBackendStateChange, op xdr.Operation) []WalletBackendStateChange {
	changes := []WalletBackendStateChange{}
	
	// Implementation for offer operations
	// This would extract OFFER_CREATED, OFFER_UPDATED, OFFER_REMOVED changes
	
	return changes
}

func (p *WalletBackendProcessor) extractPathPaymentChanges(base WalletBackendStateChange, op xdr.Operation) ([]WalletBackendStateChange, []string) {
	changes := []WalletBackendStateChange{}
	participants := []string{}
	
	// Implementation for path payment operations
	// This would extract all intermediate hops and asset conversions
	
	return changes, participants
}

func (p *WalletBackendProcessor) sortChangesByOrder(changes []WalletBackendStateChange) {
	// Sort by application order to maintain correct sequence
	for i := 0; i < len(changes)-1; i++ {
		for j := i + 1; j < len(changes); j++ {
			if changes[i].ApplicationOrder > changes[j].ApplicationOrder {
				changes[i], changes[j] = changes[j], changes[i]
			}
		}
	}
}