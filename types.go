package main

import (
	"encoding/json"
)

// Event represents a Soroban contract event
type Event struct {
	Type                     string          `json:"type"`
	Ledger                   uint64          `json:"ledger"`
	LedgerClosedAt           string          `json:"ledgerClosedAt"`
	ContractID               string          `json:"contractId"`
	ID                       string          `json:"id"`
	PagingToken              string          `json:"pagingToken"`
	Topic                    json.RawMessage `json:"topic"`
	Value                    json.RawMessage `json:"value"`
	InSuccessfulContractCall bool            `json:"inSuccessfulContractCall"`
	TxHash                   string          `json:"txHash"`
}
