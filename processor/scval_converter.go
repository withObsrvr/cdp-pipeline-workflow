package processor

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// ConvertScValToJSON converts a Soroban ScVal to a JSON-serializable format
func ConvertScValToJSON(val xdr.ScVal) (interface{}, error) {
	switch val.Type {
	case xdr.ScValTypeScvBool:
		if val.B == nil {
			return nil, fmt.Errorf("ScvBool has nil value")
		}
		return *val.B, nil

	case xdr.ScValTypeScvVoid:
		return nil, nil

	case xdr.ScValTypeScvU32:
		if val.U32 == nil {
			return nil, fmt.Errorf("ScvU32 has nil value")
		}
		return *val.U32, nil

	case xdr.ScValTypeScvI32:
		if val.I32 == nil {
			return nil, fmt.Errorf("ScvI32 has nil value")
		}
		return *val.I32, nil

	case xdr.ScValTypeScvU64:
		if val.U64 == nil {
			return nil, fmt.Errorf("ScvU64 has nil value")
		}
		return *val.U64, nil

	case xdr.ScValTypeScvI64:
		if val.I64 == nil {
			return nil, fmt.Errorf("ScvI64 has nil value")
		}
		return *val.I64, nil

	case xdr.ScValTypeScvTimepoint:
		if val.Timepoint == nil {
			return nil, fmt.Errorf("ScvTimepoint has nil value")
		}
		return map[string]interface{}{
			"type":  "timepoint",
			"value": *val.Timepoint,
		}, nil

	case xdr.ScValTypeScvDuration:
		if val.Duration == nil {
			return nil, fmt.Errorf("ScvDuration has nil value")
		}
		return map[string]interface{}{
			"type":  "duration",
			"value": *val.Duration,
		}, nil

	case xdr.ScValTypeScvU128:
		if val.U128 == nil {
			return nil, fmt.Errorf("ScvU128 has nil value")
		}
		parts := *val.U128
		return map[string]interface{}{
			"type":  "u128",
			"hi":    parts.Hi,
			"lo":    parts.Lo,
			"value": uint128ToString(parts),
		}, nil

	case xdr.ScValTypeScvI128:
		if val.I128 == nil {
			return nil, fmt.Errorf("ScvI128 has nil value")
		}
		parts := *val.I128
		return map[string]interface{}{
			"type":  "i128",
			"hi":    parts.Hi,
			"lo":    parts.Lo,
			"value": int128ToString(parts),
		}, nil

	case xdr.ScValTypeScvU256:
		if val.U256 == nil {
			return nil, fmt.Errorf("ScvU256 has nil value")
		}
		parts := *val.U256
		return map[string]interface{}{
			"type":     "u256",
			"hi_hi":    parts.HiHi,
			"hi_lo":    parts.HiLo,
			"lo_hi":    parts.LoHi,
			"lo_lo":    parts.LoLo,
			"value":    uint256ToString(parts),
			"hex":      uint256ToHex(parts),
		}, nil

	case xdr.ScValTypeScvI256:
		if val.I256 == nil {
			return nil, fmt.Errorf("ScvI256 has nil value")
		}
		parts := *val.I256
		return map[string]interface{}{
			"type":     "i256",
			"hi_hi":    parts.HiHi,
			"hi_lo":    parts.HiLo,
			"lo_hi":    parts.LoHi,
			"lo_lo":    parts.LoLo,
			"value":    int256ToString(parts),
			"hex":      int256ToHex(parts),
		}, nil

	case xdr.ScValTypeScvSymbol:
		if val.Sym == nil {
			return nil, fmt.Errorf("ScvSymbol has nil value")
		}
		return string(*val.Sym), nil

	case xdr.ScValTypeScvString:
		if val.Str == nil {
			return nil, fmt.Errorf("ScvString has nil value")
		}
		return string(*val.Str), nil

	case xdr.ScValTypeScvBytes:
		if val.Bytes == nil {
			return nil, fmt.Errorf("ScvBytes has nil value")
		}
		return map[string]interface{}{
			"type":   "bytes",
			"hex":    hex.EncodeToString(*val.Bytes),
			"base64": base64.StdEncoding.EncodeToString(*val.Bytes),
			"length": len(*val.Bytes),
		}, nil

	case xdr.ScValTypeScvAddress:
		if val.Address == nil {
			return nil, fmt.Errorf("ScvAddress has nil value")
		}
		return convertScAddress(*val.Address)

	case xdr.ScValTypeScvVec:
		if val.Vec == nil {
			return nil, fmt.Errorf("ScvVec has nil value")
		}
		vec := *val.Vec
		result := make([]interface{}, 0, len(*vec))
		for i, item := range *vec {
			converted, err := ConvertScValToJSON(item)
			if err != nil {
				result = append(result, map[string]interface{}{
					"index": i,
					"error": err.Error(),
					"type":  item.Type.String(),
				})
			} else {
				result = append(result, converted)
			}
		}
		return result, nil

	case xdr.ScValTypeScvMap:
		if val.Map == nil {
			return nil, fmt.Errorf("ScvMap has nil value")
		}
		scMap := *val.Map
		result := make(map[string]interface{})
		orderedKeys := make([]interface{}, 0, len(*scMap))
		
		for _, entry := range *scMap {
			key, keyErr := ConvertScValToJSON(entry.Key)
			value, valErr := ConvertScValToJSON(entry.Val)
			
			// Create a string representation of the key for the map
			var keyStr string
			if keyErr != nil {
				keyStr = fmt.Sprintf("error:%s", keyErr.Error())
			} else {
				keyStr = fmt.Sprintf("%v", key)
			}
			
			if valErr != nil {
				result[keyStr] = map[string]interface{}{
					"error": valErr.Error(),
					"type":  entry.Val.Type.String(),
				}
			} else {
				result[keyStr] = value
			}
			
			orderedKeys = append(orderedKeys, key)
		}
		
		return map[string]interface{}{
			"type":    "map",
			"entries": result,
			"keys":    orderedKeys,
		}, nil

	case xdr.ScValTypeScvContractInstance:
		if val.Instance == nil {
			return nil, fmt.Errorf("ScvContractInstance has nil value")
		}
		return map[string]interface{}{
			"type":   "contract_instance",
			"value":  "complex_contract_instance",
		}, nil

	case xdr.ScValTypeScvLedgerKeyContractInstance:
		return map[string]interface{}{
			"type": "ledger_key_contract_instance",
		}, nil

	case xdr.ScValTypeScvLedgerKeyNonce:
		if val.NonceKey == nil {
			return nil, fmt.Errorf("ScvLedgerKeyNonce has nil value")
		}
		nonceKey := *val.NonceKey
		return map[string]interface{}{
			"type":  "ledger_key_nonce",
			"nonce": nonceKey.Nonce,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported ScVal type: %s", val.Type.String())
	}
}

// convertScAddress converts an ScAddress to a JSON-serializable format
func convertScAddress(addr xdr.ScAddress) (interface{}, error) {
	switch addr.Type {
	case xdr.ScAddressTypeScAddressTypeAccount:
		if addr.AccountId == nil {
			return nil, fmt.Errorf("ScAddressTypeAccount has nil AccountId")
		}
		accountID := addr.AccountId.Ed25519
		accountStr, err := strkey.Encode(strkey.VersionByteAccountID, accountID[:])
		if err != nil {
			return nil, fmt.Errorf("error encoding account address: %w", err)
		}
		return map[string]interface{}{
			"type":    "account",
			"address": accountStr,
		}, nil
		
	case xdr.ScAddressTypeScAddressTypeContract:
		if addr.ContractId == nil {
			return nil, fmt.Errorf("ScAddressTypeContract has nil ContractId")
		}
		contractID := *addr.ContractId
		contractStr, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
		if err != nil {
			return nil, fmt.Errorf("error encoding contract address: %w", err)
		}
		return map[string]interface{}{
			"type":    "contract",
			"address": contractStr,
		}, nil
		
	default:
		return nil, fmt.Errorf("unknown ScAddress type: %v", addr.Type)
	}
}

// Helper functions for large integer conversions

func uint128ToString(val xdr.UInt128Parts) string {
	hi := big.NewInt(0).SetUint64(uint64(val.Hi))
	lo := big.NewInt(0).SetUint64(uint64(val.Lo))
	hi.Lsh(hi, 64)
	hi.Add(hi, lo)
	return hi.String()
}

func int128ToString(val xdr.Int128Parts) string {
	// For signed integers, we need to handle the sign bit
	hi := big.NewInt(0).SetUint64(uint64(val.Hi))
	lo := big.NewInt(0).SetUint64(uint64(val.Lo))
	
	// Check if negative (high bit set)
	if uint64(val.Hi)&(uint64(1)<<63) != 0 {
		// Two's complement for negative numbers
		hi.Sub(hi, big.NewInt(1).Lsh(big.NewInt(1), 64))
	}
	
	hi.Lsh(hi, 64)
	hi.Add(hi, lo)
	return hi.String()
}

func uint256ToString(val xdr.UInt256Parts) string {
	hiHi := big.NewInt(0).SetUint64(uint64(val.HiHi))
	hiLo := big.NewInt(0).SetUint64(uint64(val.HiLo))
	loHi := big.NewInt(0).SetUint64(uint64(val.LoHi))
	loLo := big.NewInt(0).SetUint64(uint64(val.LoLo))
	
	hiHi.Lsh(hiHi, 192)
	hiLo.Lsh(hiLo, 128)
	loHi.Lsh(loHi, 64)
	
	result := big.NewInt(0)
	result.Add(result, hiHi)
	result.Add(result, hiLo)
	result.Add(result, loHi)
	result.Add(result, loLo)
	
	return result.String()
}

func uint256ToHex(val xdr.UInt256Parts) string {
	return fmt.Sprintf("%016x%016x%016x%016x", val.HiHi, val.HiLo, val.LoHi, val.LoLo)
}

func int256ToString(val xdr.Int256Parts) string {
	hiHi := big.NewInt(0).SetUint64(uint64(val.HiHi))
	hiLo := big.NewInt(0).SetUint64(uint64(val.HiLo))
	loHi := big.NewInt(0).SetUint64(uint64(val.LoHi))
	loLo := big.NewInt(0).SetUint64(uint64(val.LoLo))
	
	// Check if negative (high bit set in HiHi)
	if uint64(val.HiHi)&(uint64(1)<<63) != 0 {
		// Two's complement for negative numbers
		hiHi.Sub(hiHi, big.NewInt(1).Lsh(big.NewInt(1), 64))
	}
	
	hiHi.Lsh(hiHi, 192)
	hiLo.Lsh(hiLo, 128)
	loHi.Lsh(loHi, 64)
	
	result := big.NewInt(0)
	result.Add(result, hiHi)
	result.Add(result, hiLo)
	result.Add(result, loHi)
	result.Add(result, loLo)
	
	return result.String()
}

func int256ToHex(val xdr.Int256Parts) string {
	return fmt.Sprintf("%016x%016x%016x%016x", val.HiHi, val.HiLo, val.LoHi, val.LoLo)
}

// GetFunctionNameFromScVal extracts a function name from various ScVal types
func GetFunctionNameFromScVal(val xdr.ScVal) string {
	switch val.Type {
	case xdr.ScValTypeScvSymbol:
		if val.Sym != nil {
			return string(*val.Sym)
		}
	case xdr.ScValTypeScvString:
		if val.Str != nil {
			return string(*val.Str)
		}
	case xdr.ScValTypeScvBytes:
		if val.Bytes != nil {
			return string(*val.Bytes)
		}
	}
	return ""
}