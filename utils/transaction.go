package utils

import (
	"encoding/hex"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// TransactionHash returns the hex-encoded hash of a transaction
func TransactionHash(tx ingest.LedgerTransaction) (*string, error) {
	transactionHash := HashToHexString(tx.Result.TransactionHash)
	return &transactionHash, nil
}

func Account(tx ingest.LedgerTransaction) (*string, error) {
	account, err := GetAccountAddressFromMuxedAccount(tx.Envelope.SourceAccount())
	if err != nil {
		return nil, err
	}

	return &account, nil

}

func TransactionEnvelope(tx ingest.LedgerTransaction) (*string, error) {
	transactionEnvelope, err := xdr.MarshalBase64(tx.Envelope)
	if err != nil {
		return nil, err
	}

	return &transactionEnvelope, nil
}

// HashToHexString is utility function that converts and xdr.Hash type to a hex string
func HashToHexString(inputHash xdr.Hash) string {
	sliceHash := inputHash[:]
	hexString := hex.EncodeToString(sliceHash)
	return hexString
}

// GetAccountAddressFromMuxedAccount takes in a muxed account and returns the address of the account
func GetAccountAddressFromMuxedAccount(account xdr.MuxedAccount) (string, error) {
	providedID := account.ToAccountId()
	pointerToID := &providedID
	return pointerToID.GetAddress()
}
