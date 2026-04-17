package stellar

import (
	"fmt"
	"strings"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// ValidateAddress checks if an address is valid
func ValidateAddress(addr string) error {
	if addr == "" {
		return fmt.Errorf("address cannot be empty")
	}
	
	_, err := strkey.Decode(strkey.VersionByteAccountID, addr)
	if err == nil {
		return nil // Valid account address
	}
	
	_, err = strkey.Decode(strkey.VersionByteContract, addr)
	if err == nil {
		return nil // Valid contract address
	}
	
	_, err = strkey.Decode(strkey.VersionByteMuxedAccount, addr)
	if err == nil {
		return nil // Valid muxed address
	}
	
	return fmt.Errorf("invalid stellar address: %s", addr)
}

// IsAccountAddress checks if address is a regular account (G...)
func IsAccountAddress(addr string) bool {
	if !strings.HasPrefix(addr, "G") {
		return false
	}
	_, err := strkey.Decode(strkey.VersionByteAccountID, addr)
	return err == nil
}

// IsContractAddress checks if address is a contract (C...)
func IsContractAddress(addr string) bool {
	if !strings.HasPrefix(addr, "C") {
		return false
	}
	_, err := strkey.Decode(strkey.VersionByteContract, addr)
	return err == nil
}

// IsMuxedAddress checks if address is muxed (M...)
func IsMuxedAddress(addr string) bool {
	if !strings.HasPrefix(addr, "M") {
		return false
	}
	_, err := strkey.Decode(strkey.VersionByteMuxedAccount, addr)
	return err == nil
}

// ExtractAddressFromScVal extracts a Stellar address from a contract ScVal
func ExtractAddressFromScVal(val xdr.ScVal) (string, error) {
	if val.Type != xdr.ScValTypeScvAddress {
		return "", fmt.Errorf("ScVal is not an address type: %v", val.Type)
	}
	
	addr := val.MustAddress()
	switch addr.Type {
	case xdr.ScAddressTypeScAddressTypeAccount:
		return addr.AccountId.Address(), nil
	case xdr.ScAddressTypeScAddressTypeContract:
		return strkey.Encode(strkey.VersionByteContract, addr.ContractId[:])
	default:
		return "", fmt.Errorf("unknown address type: %v", addr.Type)
	}
}

// MuxedAccountToAddress converts a muxed account to regular address
func MuxedAccountToAddress(muxed xdr.MuxedAccount) (string, error) {
	accountID := muxed.ToAccountId()
	return accountID.Address(), nil
}

// GetOperationSourceAccount returns the source account for an operation
func GetOperationSourceAccount(op xdr.Operation, txSource xdr.MuxedAccount) (string, error) {
	if op.SourceAccount != nil {
		return op.SourceAccount.Address(), nil
	}
	return txSource.Address(), nil
}

// EncodeContractID encodes a contract ID to its string representation
func EncodeContractID(contractID [32]byte) (string, error) {
	return strkey.Encode(strkey.VersionByteContract, contractID[:])
}

// DecodeContractID decodes a contract address string to bytes
func DecodeContractID(contractAddr string) ([32]byte, error) {
	var result [32]byte
	
	decoded, err := strkey.Decode(strkey.VersionByteContract, contractAddr)
	if err != nil {
		return result, fmt.Errorf("invalid contract address: %w", err)
	}
	
	if len(decoded) != 32 {
		return result, fmt.Errorf("invalid contract ID length: %d", len(decoded))
	}
	
	copy(result[:], decoded)
	return result, nil
}

// AddressType returns the type of a Stellar address
func AddressType(addr string) (string, error) {
	if err := ValidateAddress(addr); err != nil {
		return "", err
	}
	
	if IsAccountAddress(addr) {
		return "account", nil
	}
	if IsContractAddress(addr) {
		return "contract", nil
	}
	if IsMuxedAddress(addr) {
		return "muxed", nil
	}
	
	return "unknown", nil
}