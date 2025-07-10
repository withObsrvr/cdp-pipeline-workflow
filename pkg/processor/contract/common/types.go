package common

import (
	"encoding/json"
	"time"

	"github.com/stellar/go/xdr"
)

// ContractEvent represents an event emitted by a contract
type ContractEvent struct {
	Timestamp       time.Time       `json:"timestamp"`
	LedgerSequence  uint32          `json:"ledger_sequence"`
	TransactionHash string          `json:"transaction_hash"`
	ContractID      string          `json:"contract_id"`
	Type            string          `json:"type"`
	Topic           []xdr.ScVal     `json:"topic"`
	Data            json.RawMessage `json:"data"`
	InSuccessfulTx  bool            `json:"in_successful_tx"`
	EventIndex      int             `json:"event_index"`
	OperationIndex  int             `json:"operation_index"`
}

// ContractInvocation represents a contract invocation
type ContractInvocation struct {
	Timestamp       time.Time         `json:"timestamp"`
	LedgerSequence  uint32            `json:"ledger_sequence"`
	TransactionHash string            `json:"transaction_hash"`
	ContractID      string            `json:"contract_id"`
	FunctionName    string            `json:"function_name,omitempty"`
	InvokingAccount string            `json:"invoking_account"`
	Arguments       []json.RawMessage `json:"arguments,omitempty"`
	Successful      bool              `json:"successful"`
}
