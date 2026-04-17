package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// AccountTransaction represents a transaction involving a specific account
type AccountTransaction struct {
	Timestamp       time.Time `json:"timestamp"`
	TransactionHash string    `json:"transaction_hash"`
	LedgerSequence  uint32    `json:"ledger_sequence"`
	AccountID       string    `json:"account_id"`
	Type            string    `json:"type"`      // "stellar" or "soroban"
	Operation       string    `json:"operation"` // Operation type

	// For Stellar transactions
	Amount            string `json:"amount,omitempty"`
	AssetCode         string `json:"asset_code,omitempty"`
	CounterpartyID    string `json:"counterparty_id,omitempty"`
	TransactionStatus bool   `json:"transaction_status"`

	// For Soroban transactions
	ContractID     string                 `json:"contract_id,omitempty"`
	FunctionName   string                 `json:"function_name,omitempty"`
	ContractEvents []json.RawMessage      `json:"contract_events,omitempty"`
	Parameters     map[string]interface{} `json:"parameters,omitempty"`
}

type AccountTransactionProcessor struct {
	targetAccount     string
	startTime         time.Time
	endTime           time.Time
	processors        []Processor
	networkPassphrase string
}

func NewAccountTransactionProcessor(config map[string]interface{}) (*AccountTransactionProcessor, error) {
	var account string
	var startTime, endTime time.Time

	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	// Optional account filter
	if acc, ok := config["account"].(string); ok && acc != "" {
		account = acc
	}

	// Optional year filter
	if year, ok := config["year"].(int); ok && year > 0 {
		startTime = time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		endTime = startTime.AddDate(1, 0, 0)
	}

	return &AccountTransactionProcessor{
		targetAccount:     account,
		startTime:         startTime,
		endTime:           endTime,
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *AccountTransactionProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *AccountTransactionProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	closeTime := time.Unix(int64(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0)

	// Skip if outside target year (only if year filter is set)
	if !p.startTime.IsZero() && !p.endTime.IsZero() {
		if closeTime.Before(p.startTime) || closeTime.After(p.endTime) {
			return nil
		}
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
		ledgerSequence := ledgerCloseMeta.LedgerSequence()

		// Process the transaction if it involves our target account (only if account filter is set)
		if p.targetAccount == "" || p.isAccountInvolved(tx, p.targetAccount) {
			accTx, err := p.processTransaction(tx, closeTime, ledgerSequence)
			if err != nil {
				log.Printf("Error processing transaction: %v", err)
				continue
			}

			// Forward to next processors
			if err := p.forwardTransaction(ctx, accTx); err != nil {
				return fmt.Errorf("error forwarding transaction: %w", err)
			}
		}
	}

	return nil
}

func (p *AccountTransactionProcessor) isAccountInvolved(tx ingest.LedgerTransaction, account string) bool {
	// Check source account
	if tx.Envelope.SourceAccount().ToAccountId().Address() == account {
		return true
	}

	// Check operations
	for _, op := range tx.Envelope.Operations() {
		// Check operation source account if present
		if op.SourceAccount != nil && op.SourceAccount.Address() == account {
			return true
		}

		// Check destination account in relevant operations
		switch op.Body.Type {
		case xdr.OperationTypePayment:
			if op.Body.PaymentOp.Destination.Address() == account {
				return true
			}
		case xdr.OperationTypeCreateAccount:
			if op.Body.CreateAccountOp.Destination.Address() == account {
				return true
			}
		case xdr.OperationTypeAccountMerge:
			if op.Body.Destination.Address() == account {
				return true
			}
		case xdr.OperationTypeInvokeHostFunction:
			// Get the invoking account
			var invokingAccount xdr.AccountId
			if op.SourceAccount != nil {
				invokingAccount = op.SourceAccount.ToAccountId()
			} else {
				invokingAccount = tx.Envelope.SourceAccount().ToAccountId()
			}
			if invokingAccount.Address() == account {
				return true
			}
		}
	}

	return false
}

func (p *AccountTransactionProcessor) processTransaction(tx ingest.LedgerTransaction, closeTime time.Time, ledgerSequence uint32) (*AccountTransaction, error) {
	// Get the source account - either target account or transaction source
	sourceAccount := p.targetAccount
	if sourceAccount == "" {
		sourceAccount = tx.Envelope.SourceAccount().ToAccountId().Address()
	}

	accTx := &AccountTransaction{
		Timestamp:         closeTime,
		TransactionHash:   tx.Result.TransactionHash.HexString(),
		LedgerSequence:    ledgerSequence,
		AccountID:         sourceAccount,
		TransactionStatus: tx.Result.Successful(),
	}

	// Process operations
	for _, op := range tx.Envelope.Operations() {
		// Get operation source if available, otherwise use transaction source
		opSourceAccount := sourceAccount
		if op.SourceAccount != nil {
			opSourceAccount = op.SourceAccount.Address()
		}

		accTx.AccountID = opSourceAccount

		switch op.Body.Type {
		case xdr.OperationTypePayment:
			accTx.Type = "stellar"
			accTx.Operation = "payment"
			payment := op.Body.MustPaymentOp()
			accTx.Amount = amount.String(payment.Amount)
			accTx.AssetCode = payment.Asset.StringCanonical()
			accTx.CounterpartyID = payment.Destination.Address()

		case xdr.OperationTypeCreateAccount:
			accTx.Type = "stellar"
			accTx.Operation = "create_account"
			create := op.Body.MustCreateAccountOp()
			accTx.Amount = amount.String(create.StartingBalance)
			accTx.AssetCode = "XLM"
			accTx.CounterpartyID = create.Destination.Address()

		case xdr.OperationTypePathPaymentStrictReceive:
			accTx.Type = "stellar"
			accTx.Operation = "path_payment_strict_receive"
			pathPayment := op.Body.MustPathPaymentStrictReceiveOp()
			accTx.Amount = amount.String(pathPayment.DestAmount)
			accTx.AssetCode = pathPayment.DestAsset.StringCanonical()
			accTx.CounterpartyID = pathPayment.Destination.Address()

		case xdr.OperationTypePathPaymentStrictSend:
			accTx.Type = "stellar"
			accTx.Operation = "path_payment_strict_send"
			pathPayment := op.Body.MustPathPaymentStrictSendOp()
			accTx.Amount = amount.String(pathPayment.SendAmount)
			accTx.AssetCode = pathPayment.DestAsset.StringCanonical()
			accTx.CounterpartyID = pathPayment.Destination.Address()

		case xdr.OperationTypeManageSellOffer:
			accTx.Type = "stellar"
			accTx.Operation = "manage_sell_offer"
			offer := op.Body.MustManageSellOfferOp()
			accTx.Amount = amount.String(offer.Amount)
			accTx.AssetCode = offer.Selling.StringCanonical()

		case xdr.OperationTypeManageBuyOffer:
			accTx.Type = "stellar"
			accTx.Operation = "manage_buy_offer"
			offer := op.Body.MustManageBuyOfferOp()
			accTx.Amount = amount.String(offer.BuyAmount)
			accTx.AssetCode = offer.Buying.StringCanonical()

		case xdr.OperationTypeCreatePassiveSellOffer:
			accTx.Type = "stellar"
			accTx.Operation = "create_passive_sell_offer"
			offer := op.Body.MustCreatePassiveSellOfferOp()
			accTx.Amount = amount.String(offer.Amount)
			accTx.AssetCode = offer.Selling.StringCanonical()

		case xdr.OperationTypeChangeTrust:
			accTx.Type = "stellar"
			accTx.Operation = "change_trust"
			trust := op.Body.MustChangeTrustOp()
			if trust.Line.Type != xdr.AssetTypeAssetTypePoolShare {
				accTx.AssetCode = trust.Line.ToAsset().StringCanonical()
				accTx.Amount = amount.String(trust.Limit)
			} else {
				accTx.AssetCode = "liquidity_pool_shares"
				accTx.Amount = amount.String(trust.Limit)
			}

		case xdr.OperationTypeAllowTrust:
			accTx.Type = "stellar"
			accTx.Operation = "allow_trust"
			trust := op.Body.MustAllowTrustOp()
			accTx.CounterpartyID = trust.Trustor.Address()
			accTx.AssetCode = trust.Asset.GoString()

		case xdr.OperationTypeAccountMerge:
			accTx.Type = "stellar"
			accTx.Operation = "account_merge"
			accTx.CounterpartyID = op.Body.MustDestination().ToAccountId().Address()

		case xdr.OperationTypeManageData:
			accTx.Type = "stellar"
			accTx.Operation = "manage_data"
			data := op.Body.MustManageDataOp()
			accTx.Parameters = map[string]interface{}{
				"data_name": string(data.DataName),
				"has_value": data.DataValue != nil,
			}

		case xdr.OperationTypeInvokeHostFunction:
			accTx.Type = "soroban"
			hostFn := op.Body.MustInvokeHostFunctionOp()

			// Get the invoking account
			var invokingAccount xdr.AccountId
			if op.SourceAccount != nil {
				invokingAccount = op.SourceAccount.ToAccountId()
			} else {
				invokingAccount = tx.Envelope.SourceAccount().ToAccountId()
			}
			accTx.CounterpartyID = invokingAccount.Address()

			// Set operation based on host function type
			switch hostFn.HostFunction.Type {
			case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
				accTx.Operation = "invoke_contract"
				contract := hostFn.HostFunction.MustInvokeContract()
				if contractID, err := strkey.Encode(strkey.VersionByteContract, contract.ContractAddress.ContractId[:]); err == nil {
					accTx.ContractID = contractID
				}
				if len(contract.Args) > 0 {
					if sym, ok := contract.Args[0].GetSym(); ok {
						accTx.FunctionName = string(sym)
					}
				}
			case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
				accTx.Operation = "create_contract"
				create := hostFn.HostFunction.MustCreateContract()
				accTx.Parameters = map[string]interface{}{
					"executable_type": create.Executable.Type.String(),
				}
			case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
				accTx.Operation = "upload_contract_wasm"
			default:
				accTx.Operation = hostFn.HostFunction.Type.String()
			}

			// Add diagnostic events if available
			events, err := tx.GetDiagnosticEvents()
			if err == nil {
				for _, event := range events {
					if eventBytes, err := json.Marshal(event); err == nil {
						accTx.ContractEvents = append(accTx.ContractEvents, eventBytes)
					}
				}
			}

		default:
			accTx.Type = "stellar"
			accTx.Operation = op.Body.Type.String()
		}
	}

	return accTx, nil
}

func (p *AccountTransactionProcessor) processOperationDetails(accTx *AccountTransaction, op xdr.Operation) {
	switch op.Body.Type {
	case xdr.OperationTypePayment:
		payment := op.Body.MustPaymentOp()
		accTx.Amount = amount.String(payment.Amount)
		accTx.AssetCode = payment.Asset.StringCanonical()
		accTx.CounterpartyID = payment.Destination.Address()

	case xdr.OperationTypeCreateAccount:
		create := op.Body.MustCreateAccountOp()
		accTx.Amount = amount.String(create.StartingBalance)
		accTx.AssetCode = "XLM"
		accTx.CounterpartyID = create.Destination.Address()

		// Add more operation types as needed

	}
}

func (p *AccountTransactionProcessor) forwardTransaction(ctx context.Context, tx *AccountTransaction) error {
	jsonBytes, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("error marshaling transaction: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}
