# Contract Invocation Enhancement Examples

This document provides examples of the enhanced contract invocation processor output, showing before and after the improvements.

## Example 1: DEX Swap Function

### Before Enhancement
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "ledger_sequence": 51234567,
  "transaction_hash": "abc123def456...",
  "contract_id": "CCFZRNQVAY52P7LRY7GLA2R2XQMWMYYB6WZ7EKNO3BV3LP3QGXFB4VKJ",
  "invoking_account": "GABC123...",
  "function_name": "",  // Often empty due to limited extraction logic
  "successful": true
}
```

### After Enhancement
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "ledger_sequence": 51234567,
  "transaction_hash": "abc123def456...",
  "contract_id": "CCFZRNQVAY52P7LRY7GLA2R2XQMWMYYB6WZ7EKNO3BV3LP3QGXFB4VKJ",
  "invoking_account": "GABC123...",
  "function_name": "swap",
  "arguments": [
    "\"swap\"",
    "{\"type\":\"i128\",\"hi\":0,\"lo\":1000000000,\"value\":\"1000000000\"}",
    "{\"type\":\"i128\",\"hi\":0,\"lo\":950000000,\"value\":\"950000000\"}",
    "[{\"type\":\"contract\",\"address\":\"CCTOKEN1...\"},{\"type\":\"contract\",\"address\":\"CCTOKEN2...\"}]",
    "{\"type\":\"account\",\"address\":\"GRECIPIENT...\"}"
  ],
  "arguments_decoded": {
    "arg_0": "swap",
    "arg_1": {
      "type": "i128",
      "hi": 0,
      "lo": 1000000000,
      "value": "1000000000"
    },
    "arg_2": {
      "type": "i128",
      "hi": 0,
      "lo": 950000000,
      "value": "950000000"
    },
    "arg_3": [
      {
        "type": "contract",
        "address": "CCTOKEN1..."
      },
      {
        "type": "contract",
        "address": "CCTOKEN2..."
      }
    ],
    "arg_4": {
      "type": "account",
      "address": "GRECIPIENT..."
    }
  },
  "successful": true
}
```

## Example 2: Liquidity Pool Deposit

### After Enhancement
```json
{
  "timestamp": "2024-01-15T11:45:00Z",
  "ledger_sequence": 51234789,
  "transaction_hash": "def789ghi012...",
  "contract_id": "CCLIQUIDITYPOOL...",
  "invoking_account": "GDEPOSITOR...",
  "function_name": "deposit",
  "arguments": [
    "\"deposit\"",
    "{\"type\":\"i128\",\"value\":\"50000000000\"}",
    "{\"type\":\"i128\",\"value\":\"25000000000\"}",
    "{\"type\":\"i128\",\"value\":\"40000000000\"}",
    "{\"type\":\"u64\",\"value\":1705325100}"
  ],
  "arguments_decoded": {
    "arg_0": "deposit",
    "arg_1": {
      "type": "i128",
      "value": "50000000000"  // max_amount_a
    },
    "arg_2": {
      "type": "i128",
      "value": "25000000000"  // max_amount_b
    },
    "arg_3": {
      "type": "i128",
      "value": "40000000000"  // min_liquidity_amount
    },
    "arg_4": {
      "type": "u64",
      "value": 1705325100      // deadline timestamp
    }
  },
  "successful": true
}
```

## Example 3: Token Transfer

### After Enhancement
```json
{
  "timestamp": "2024-01-15T12:00:00Z",
  "ledger_sequence": 51234890,
  "transaction_hash": "xyz789abc123...",
  "contract_id": "CCTOKEN123...",
  "invoking_account": "GSENDER...",
  "function_name": "transfer",
  "arguments": [
    "\"transfer\"",
    "{\"type\":\"account\",\"address\":\"GSENDER...\"}",
    "{\"type\":\"account\",\"address\":\"GRECEIVER...\"}",
    "{\"type\":\"i128\",\"value\":\"1000000\"}"
  ],
  "arguments_decoded": {
    "arg_0": "transfer",
    "arg_1": {
      "type": "account",
      "address": "GSENDER..."     // from
    },
    "arg_2": {
      "type": "account",
      "address": "GRECEIVER..."   // to
    },
    "arg_3": {
      "type": "i128",
      "value": "1000000"          // amount
    }
  },
  "successful": true
}
```

## Example 4: Complex Map Argument

### After Enhancement
```json
{
  "timestamp": "2024-01-15T13:15:00Z",
  "ledger_sequence": 51235001,
  "transaction_hash": "map123example...",
  "contract_id": "CCCOMPLEX...",
  "invoking_account": "GCALLER...",
  "function_name": "update_config",
  "arguments": [
    "\"update_config\"",
    "{\"type\":\"map\",\"entries\":{\"fee_rate\":{\"type\":\"u32\",\"value\":300},\"admin\":{\"type\":\"account\",\"address\":\"GADMIN...\"},\"paused\":{\"type\":\"bool\",\"value\":false}},\"keys\":[\"fee_rate\",\"admin\",\"paused\"]}"
  ],
  "arguments_decoded": {
    "arg_0": "update_config",
    "arg_1": {
      "type": "map",
      "entries": {
        "fee_rate": {
          "type": "u32",
          "value": 300
        },
        "admin": {
          "type": "account",
          "address": "GADMIN..."
        },
        "paused": false
      },
      "keys": ["fee_rate", "admin", "paused"]
    }
  },
  "successful": true
}
```

## Example 5: Vector of Addresses

### After Enhancement
```json
{
  "timestamp": "2024-01-15T14:30:00Z",
  "ledger_sequence": 51235123,
  "transaction_hash": "vec456example...",
  "contract_id": "CCMULTISIG...",
  "invoking_account": "GINITIATOR...",
  "function_name": "add_signers",
  "arguments": [
    "\"add_signers\"",
    "[{\"type\":\"account\",\"address\":\"GSIGNER1...\"},{\"type\":\"account\",\"address\":\"GSIGNER2...\"},{\"type\":\"account\",\"address\":\"GSIGNER3...\"}]",
    "{\"type\":\"u32\",\"value\":2}"
  ],
  "arguments_decoded": {
    "arg_0": "add_signers",
    "arg_1": [
      {
        "type": "account",
        "address": "GSIGNER1..."
      },
      {
        "type": "account",
        "address": "GSIGNER2..."
      },
      {
        "type": "account",
        "address": "GSIGNER3..."
      }
    ],
    "arg_2": {
      "type": "u32",
      "value": 2  // threshold
    }
  },
  "successful": true
}
```

## Data Type Examples

### Numeric Types
- **u32/i32**: 32-bit unsigned/signed integers
- **u64/i64**: 64-bit unsigned/signed integers  
- **u128/i128**: 128-bit integers with hi/lo parts
- **u256/i256**: 256-bit integers with 4 parts

### Address Types
- **account**: Stellar account (G...)
- **contract**: Contract address (C...)

### Complex Types
- **vec**: Array of values
- **map**: Key-value pairs
- **bytes**: Binary data (shown as hex and base64)
- **symbol**: Contract symbols
- **string**: UTF-8 strings

## Benefits of Enhancement

1. **Complete Function Visibility**: Always know which function was called
2. **Full Argument Access**: All parameters are captured and decoded
3. **Type Information**: Each argument includes its Soroban type
4. **Human Readable**: Complex types are converted to JSON
5. **Backward Compatible**: Existing consumers still work

## Usage Tips

1. **Filtering by Function**: Easy to filter for specific operations
2. **Argument Analysis**: Can analyze parameter patterns
3. **Type Safety**: Type information helps with validation
4. **Debugging**: Complete call information aids troubleshooting
5. **Monitoring**: Set alerts on specific function/argument combinations