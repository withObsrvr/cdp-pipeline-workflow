package processor

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// LedgerToJSONProcessor converts XDR LedgerCloseMeta to AWS-like JSON structure
type LedgerToJSONProcessor struct {
	subscribers       []Processor
	networkPassphrase string
}

// NewLedgerToJSONProcessor creates a new ledger to JSON processor
func NewLedgerToJSONProcessor(config map[string]interface{}) (*LedgerToJSONProcessor, error) {
	networkPassphrase := "Public Global Stellar Network ; September 2015" // Default mainnet

	if val, ok := config["network_passphrase"].(string); ok {
		networkPassphrase = val
	}

	return &LedgerToJSONProcessor{
		subscribers:       []Processor{},
		networkPassphrase: networkPassphrase,
	}, nil
}

// ProcessorLedgerToJSON creates a new ledger to JSON processor from config
func ProcessorLedgerToJSON(config map[string]interface{}) Processor {
	proc, _ := NewLedgerToJSONProcessor(config)
	return proc
}

// Subscribe adds a subscriber to receive processed messages
func (p *LedgerToJSONProcessor) Subscribe(subscriber Processor) {
	p.subscribers = append(p.subscribers, subscriber)
}

// Process converts ledger to AWS-like JSON structure
func (p *LedgerToJSONProcessor) Process(ctx context.Context, msg Message) error {
	// Extract LedgerCloseMeta
	ledgerCloseMeta, ok := msg.Payload.(*xdr.LedgerCloseMeta)
	if !ok {
		if lcm, ok := msg.Payload.(xdr.LedgerCloseMeta); ok {
			ledgerCloseMeta = &lcm
		} else {
			return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
		}
	}

	// Build AWS-like structure
	output, err := p.buildLedgerJSON(ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("failed to build ledger JSON: %w", err)
	}

	// Convert to JSON bytes
	jsonBytes, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("failed to marshal output: %w", err)
	}

	// Create output message
	outputMsg := Message{
		Payload:  jsonBytes,
		Metadata: msg.Metadata,
	}

	// Forward to all subscribers
	for _, subscriber := range p.subscribers {
		if err := subscriber.Process(ctx, outputMsg); err != nil {
			return fmt.Errorf("error in subscriber processing: %w", err)
		}
	}

	return nil
}

func (p *LedgerToJSONProcessor) buildLedgerJSON(lcm *xdr.LedgerCloseMeta) (map[string]interface{}, error) {
	header := lcm.LedgerHeaderHistoryEntry()
	
	// Top-level ledger fields matching AWS schema
	output := map[string]interface{}{
		"sequence":                      lcm.LedgerSequence(),
		"ledger_hash":                   lcm.LedgerHash().HexString(),
		"previous_ledger_hash":          header.Header.PreviousLedgerHash.HexString(),
		"closed_at":                     header.Header.ScpValue.CloseTime * 1000, // Convert to milliseconds
		"protocol_version":              header.Header.LedgerVersion,
		"total_coins":                   header.Header.TotalCoins,
		"fee_pool":                      header.Header.FeePool,
		"base_fee":                      header.Header.BaseFee,
		"base_reserve":                  header.Header.BaseReserve,
		"max_tx_set_size":               header.Header.MaxTxSetSize,
		"successful_transaction_count":  0,
		"failed_transaction_count":      0,
	}

	// Add optional fields
	if lcm.V1 != nil {
		output["soroban_fee_write_1kb"] = lcm.V1.Ext.V1.SorobanFeeWrite1Kb
	}

	// Process transactions
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		return nil, fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	transactions := []map[string]interface{}{}
	successfulCount := 0
	failedCount := 0

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading transaction: %w", err)
		}

		txJSON := p.buildTransactionJSON(&tx)
		transactions = append(transactions, txJSON)

		if tx.Result.Successful() {
			successfulCount++
		} else {
			failedCount++
		}
	}

	output["successful_transaction_count"] = successfulCount
	output["failed_transaction_count"] = failedCount
	output["transactions"] = transactions

	return output, nil
}

func (p *LedgerToJSONProcessor) buildTransactionJSON(tx *ingest.LedgerTransaction) map[string]interface{} {
	txJSON := map[string]interface{}{
		"transaction_hash":    tx.Result.TransactionHash.HexString(),
		"account":            tx.Envelope.SourceAccount().ToAccountId().Address(),
		"account_sequence":   tx.Envelope.SeqNum(),
		"max_fee":            tx.Envelope.Fee(),
		"fee_charged":        int64(tx.Result.Result.FeeCharged),
		"successful":         tx.Result.Successful(),
		"operation_count":    len(tx.Envelope.Operations()),
	}

	// Add optional fields
	if tx.Envelope.IsFeeBump() {
		innerHash := tx.Result.InnerHash()
		if innerHash != (xdr.Hash{}) {
			txJSON["inner_transaction_hash"] = innerHash.HexString()
		}
	}

	// Add memo
	memo := tx.Envelope.Memo()
	switch memo.Type {
	case xdr.MemoTypeMemoNone:
		// No memo
	case xdr.MemoTypeMemoText:
		txJSON["memo_type"] = "text"
		txJSON["memo"] = string(memo.MustText())
	case xdr.MemoTypeMemoId:
		txJSON["memo_type"] = "id"
		txJSON["memo"] = fmt.Sprintf("%d", memo.MustId())
	case xdr.MemoTypeMemoHash:
		txJSON["memo_type"] = "hash"
		hash := memo.MustHash()
		txJSON["memo"] = base64.StdEncoding.EncodeToString(hash[:])
	case xdr.MemoTypeMemoReturn:
		txJSON["memo_type"] = "return"
		retHash := memo.MustRetHash()
		txJSON["memo"] = base64.StdEncoding.EncodeToString(retHash[:])
	}

	// Add time bounds
	if tb := tx.Envelope.TimeBounds(); tb != nil {
		if tb.MinTime > 0 {
			txJSON["time_bounds_lower"] = tb.MinTime
		}
		if tb.MaxTime > 0 {
			txJSON["time_bounds_upper"] = tb.MaxTime
		}
	}

	// Add transaction result code
	if !tx.Result.Successful() {
		// Get the appropriate result code based on transaction type
		var code string
		switch tx.Result.Result.Result.Code {
		case xdr.TransactionResultCodeTxSuccess:
			code = "txSUCCESS"
		case xdr.TransactionResultCodeTxFailed:
			code = "txFAILED"
		case xdr.TransactionResultCodeTxTooEarly:
			code = "txTOO_EARLY"
		case xdr.TransactionResultCodeTxTooLate:
			code = "txTOO_LATE"
		case xdr.TransactionResultCodeTxMissingOperation:
			code = "txMISSING_OPERATION"
		case xdr.TransactionResultCodeTxBadSeq:
			code = "txBAD_SEQ"
		case xdr.TransactionResultCodeTxBadAuth:
			code = "txBAD_AUTH"
		case xdr.TransactionResultCodeTxInsufficientBalance:
			code = "txINSUFFICIENT_BALANCE"
		case xdr.TransactionResultCodeTxNoAccount:
			code = "txNO_ACCOUNT"
		case xdr.TransactionResultCodeTxInsufficientFee:
			code = "txINSUFFICIENT_FEE"
		case xdr.TransactionResultCodeTxBadAuthExtra:
			code = "txBAD_AUTH_EXTRA"
		case xdr.TransactionResultCodeTxInternalError:
			code = "txINTERNAL_ERROR"
		default:
			code = fmt.Sprintf("UNKNOWN_%d", tx.Result.Result.Result.Code)
		}
		txJSON["transaction_result_code"] = code
	}

	// Add Soroban fields
	if tx.Envelope.IsFeeBump() {
		// Handle fee bump - get the fee from the envelope
		txJSON["new_max_fee"] = tx.Envelope.Fee()
	}

	// Process operations
	operations := []map[string]interface{}{}
	for i, op := range tx.Envelope.Operations() {
		opJSON := p.buildOperationJSON(tx, op, i)
		operations = append(operations, opJSON)
	}
	txJSON["operations"] = operations

	// Extract events (transaction level)
	if events := p.extractTransactionEvents(tx); len(events) > 0 {
		txJSON["events"] = events
	}

	return txJSON
}

func (p *LedgerToJSONProcessor) buildOperationJSON(tx *ingest.LedgerTransaction, op xdr.Operation, index int) map[string]interface{} {
	opJSON := map[string]interface{}{
		"id":             fmt.Sprintf("%s-%d", tx.Result.TransactionHash.HexString(), index),
		"type":           op.Body.Type.String(),
	}
	
	// Extract operation result if transaction was successful
	if tx.Result.Successful() {
		// Get operation results based on transaction result type
		switch tx.Result.Result.Result.Code {
		case xdr.TransactionResultCodeTxSuccess, xdr.TransactionResultCodeTxFailed:
			if results := tx.Result.Result.Result.Results; results != nil && len(*results) > index {
				opResult := (*results)[index]
				opJSON["result_code"] = opResult.Code.String()
			}
		}
	}

	// Add source account if different from transaction source
	if op.SourceAccount != nil {
		opJSON["source_account"] = op.SourceAccount.ToAccountId().Address()
		// Check if it's a muxed account
		if op.SourceAccount.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
			// Encode muxed account ID
			muxedBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(muxedBytes, uint64(op.SourceAccount.Med25519.Id))
			opJSON["source_account_muxed"] = strkey.MustEncode(strkey.VersionByteMuxedAccount, muxedBytes)
		}
	}

	// Marshal operation body as base64 for simplicity
	bodyBytes, _ := op.Body.MarshalBinary()
	opJSON["body"] = base64.StdEncoding.EncodeToString(bodyBytes)

	// Extract ledger entry changes for this operation
	if changes := p.extractOperationChanges(tx, index); len(changes) > 0 {
		opJSON["ledger_entry_changes"] = changes
	}

	// Extract operation events
	if events := p.extractOperationEvents(tx, index); len(events) > 0 {
		opJSON["events"] = events
	}

	return opJSON
}

func (p *LedgerToJSONProcessor) extractTransactionEvents(tx *ingest.LedgerTransaction) []map[string]interface{} {
	events := []map[string]interface{}{}
	
	// Use SDK helper to get events
	txEvents, err := tx.GetTransactionEvents()
	if err != nil {
		return events
	}

	for i, event := range txEvents.TransactionEvents {
		eventJSON := map[string]interface{}{
			"id":   fmt.Sprintf("%s-tx-%d", tx.Result.TransactionHash.HexString(), i),
			"type": "contract",
			"in_successful_contract_call": tx.Result.Successful(),
		}

		// TransactionEvent structure is different from ContractEvent
		// Extract contract ID if available from the event's contract field
		if event.Event.ContractId != nil {
			eventJSON["contract_id"] = strkey.MustEncode(strkey.VersionByteContract, (*event.Event.ContractId)[:])
		}

		// Add topics and data as base64 from the event body
		if event.Event.Body.V == 0 && event.Event.Body.V0 != nil {
			if len(event.Event.Body.V0.Topics) > 0 {
				topics, _ := json.Marshal(event.Event.Body.V0.Topics)
				eventJSON["topics"] = base64.StdEncoding.EncodeToString(topics)
			}
			
			// ScVal is not a pointer, always marshal it
			data, _ := event.Event.Body.V0.Data.MarshalBinary()
			eventJSON["data"] = base64.StdEncoding.EncodeToString(data)
		}

		events = append(events, eventJSON)
	}

	return events
}

func (p *LedgerToJSONProcessor) extractOperationEvents(tx *ingest.LedgerTransaction, opIndex int) []map[string]interface{} {
	events := []map[string]interface{}{}
	
	// Use SDK helper to get events
	txEvents, err := tx.GetTransactionEvents()
	if err != nil || len(txEvents.OperationEvents) <= opIndex {
		return events
	}

	for i, event := range txEvents.OperationEvents[opIndex] {
		eventJSON := map[string]interface{}{
			"id":   fmt.Sprintf("%s-op%d-%d", tx.Result.TransactionHash.HexString(), opIndex, i),
			"type": "contract",
			"in_successful_contract_call": tx.Result.Successful(),
		}

		// Operation events have ContractEvent type
		if event.ContractId != nil {
			eventJSON["contract_id"] = strkey.MustEncode(strkey.VersionByteContract, (*event.ContractId)[:])
		}

		// Add topics and data as base64 from the event body
		if event.Body.V == 0 && event.Body.V0 != nil {
			if len(event.Body.V0.Topics) > 0 {
				topics, _ := json.Marshal(event.Body.V0.Topics)
				eventJSON["topics"] = base64.StdEncoding.EncodeToString(topics)
			}
			
			// ScVal is not a pointer, always marshal it
			data, _ := event.Body.V0.Data.MarshalBinary()
			eventJSON["data"] = base64.StdEncoding.EncodeToString(data)
		}

		events = append(events, eventJSON)
	}

	return events
}

func (p *LedgerToJSONProcessor) extractOperationChanges(tx *ingest.LedgerTransaction, opIndex int) []map[string]interface{} {
	changes := []map[string]interface{}{}
	
	// Get operation changes
	opChanges, err := tx.GetOperationChanges(uint32(opIndex))
	if err != nil {
		return changes
	}

	for i, change := range opChanges {
		changeJSON := map[string]interface{}{
			"id": fmt.Sprintf("%s-op%d-change%d", tx.Result.TransactionHash.HexString(), opIndex, i),
		}

		// Determine change type
		if change.Pre != nil && change.Post == nil {
			changeJSON["change_type"] = "removed"
		} else if change.Pre == nil && change.Post != nil {
			changeJSON["change_type"] = "created"
		} else {
			changeJSON["change_type"] = "updated"
		}

		// Add ledger entry type and data
		entry := change.Post
		if entry == nil {
			entry = change.Pre
		}

		if entry != nil {
			changeJSON["ledger_entry_type"] = entry.Data.Type.String()
			
			// Marshal entry data as base64
			entryData, _ := entry.Data.MarshalBinary()
			changeJSON["entry_data"] = base64.StdEncoding.EncodeToString(entryData)

			// Add last modified ledger sequence if available
			if entry.LastModifiedLedgerSeq != 0 {
				changeJSON["last_modified_ledger_sequence"] = entry.LastModifiedLedgerSeq
			}
		}

		changes = append(changes, changeJSON)
	}

	return changes
}