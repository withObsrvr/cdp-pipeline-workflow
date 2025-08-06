# One-Liner Pipeline Implementation

## Executive Summary

This document outlines the implementation plan for adding one-liner pipeline syntax to flowctl, enabling users to define and run data pipelines with a single command. This feature represents Level 0 of the progressive complexity model from our vision document.

**Goal**: Enable pipelines to be defined and executed in a single line, reducing time-to-value from minutes to seconds.

**Example**: 
```bash
flowctl run "stellar://mainnet | filter-payments | postgres://analytics"
```

**Key Benefits**:
- Zero configuration for simple pipelines
- Intuitive Unix-style pipe syntax
- 100% backward compatible with existing YAML configs
- Progressive enhancement path to complex pipelines

## Syntax Specification

### Grammar Definition (EBNF)

```ebnf
pipeline         = source ("|" processor)* "|" consumer
source           = protocol-url | alias
processor        = alias | inline-filter | protocol-url
consumer         = protocol-url | alias
protocol-url     = protocol "://" location ["?" parameters]
inline-filter    = "filter" expression
alias            = identifier
expression       = field operator value [logical-op expression]*
```

### Core Components

#### 1. Sources
```bash
stellar://network                    # Stellar network (mainnet/testnet/futurenet)
s3://bucket/path                     # S3 storage
gs://bucket/path                     # Google Cloud Storage
file:///absolute/path                # Local filesystem
kafka://broker:port/topic            # Kafka topic
http://api.example.com/endpoint      # HTTP/REST endpoint
```

#### 2. Processors
```bash
filter-payments                      # Alias for payment filtering
filter amount > 100                  # Inline filter expression
contract-data                        # Contract data processor
latest-ledger                        # Latest ledger processor
transform field=value                # Generic transformation
```

#### 3. Consumers
```bash
postgres://user:pass@host/db         # PostgreSQL
mongo://host:port/database           # MongoDB
redis://host:port                    # Redis
parquet://path/to/output            # Parquet files
gs://bucket/output                   # Cloud storage
kafka://broker:port/topic            # Kafka topic
stdout                               # Console output
```

### Inline Filter Syntax

```bash
# Comparison operators
filter amount > 100
filter status == "active"
filter created_at >= "2024-01-01"

# Logical operators
filter amount > 100 && type == "payment"
filter status == "pending" || retry_count > 3

# Field existence
filter has:metadata
filter !has:error

# Array operations
filter tags contains "important"
filter assets.length > 0
```

## Architecture Design

### Component Overview

```
┌─────────────────┐
│   CLI Input     │
│ "source|proc|sink" │
└────────┬────────┘
         │
┌────────▼────────┐
│ Pipeline Parser │ ← New Component
├─────────────────┤
│ • Tokenization  │
│ • URL Parsing   │
│ • Alias Lookup  │
└────────┬────────┘
         │
┌────────▼────────┐
│  Config Builder │ ← New Component
├─────────────────┤
│ • V2 Generation │
│ • Defaults      │
│ • Validation    │
└────────┬────────┘
         │
┌────────▼────────┐
│  V2 Config      │
│    Loader       │ ← Existing
└────────┬────────┘
         │
┌────────▼────────┐
│Pipeline Runner  │ ← Existing
└─────────────────┘
```

### Integration Points

1. **CLI Layer** (`internal/cli/cmd/run.go`)
   - Detect one-liner vs file input
   - Route to appropriate parser

2. **Parser Module** (`internal/cli/pipeline/parser.go`)
   - Parse pipeline expressions
   - Extract configurations from URLs
   - Resolve aliases

3. **Config Builder** (`internal/cli/pipeline/builder.go`)
   - Convert parsed expression to v2 config
   - Apply intelligent defaults
   - Validate completeness

## Implementation Details

### 1. Modify CLI Run Command

```go
// internal/cli/cmd/run.go
func runPipeline(cmd *cobra.Command, args []string) error {
    input := args[0]
    
    // Check if input is a file
    if _, err := os.Stat(input); err == nil {
        // Existing file-based flow
        return runFromFile(input)
    }
    
    // Try parsing as pipeline expression
    if strings.Contains(input, "|") {
        return runFromExpression(input)
    }
    
    // Not a file or expression
    return fmt.Errorf("input must be a config file or pipeline expression")
}
```

### 2. Pipeline Parser Module

```go
// internal/cli/pipeline/parser.go
package pipeline

type Expression struct {
    Source     Component
    Processors []Component
    Consumer   Component
}

type Component struct {
    Raw       string
    Type      ComponentType
    Protocol  string
    Location  string
    Config    map[string]interface{}
}

func Parse(expression string) (*Expression, error) {
    // Implementation details in full document
}
```

### 3. Protocol Registry

```go
// internal/cli/pipeline/protocols.go
type ProtocolHandler interface {
    Parse(url string) (componentType string, config map[string]interface{}, error)
    Validate(config map[string]interface{}) error
}

var protocolHandlers = map[string]ProtocolHandler{
    "stellar":  &StellarProtocolHandler{},
    "s3":       &S3ProtocolHandler{},
    "gs":       &GCSProtocolHandler{},
    "postgres": &PostgresProtocolHandler{},
    // ... more handlers
}
```

## Protocol Mappings

### Source Protocols

| Protocol | Component Type | URL Format | Config Mapping |
|----------|---------------|------------|----------------|
| `stellar://` | CaptiveCoreInboundAdapter | `stellar://network` | `network: <network>` |
| `s3://` | S3BufferedStorageSourceAdapter | `s3://bucket/path?region=us-east-1` | `bucket_name: <bucket>`<br>`path_prefix: <path>`<br>`region: <region>` |
| `gs://` | BufferedStorageSourceAdapter | `gs://bucket/path` | `bucket_name: <bucket>`<br>`path_prefix: <path>` |
| `file://` | FSBufferedStorageSourceAdapter | `file:///absolute/path` | `path: <path>` |
| `kafka://` | KafkaSourceAdapter | `kafka://broker:9092/topic` | `brokers: [<broker>]`<br>`topic: <topic>` |
| `http://` | HTTPSourceAdapter | `http://api.example.com/path` | `endpoint: <url>` |

### Consumer Protocols

| Protocol | Component Type | URL Format | Config Mapping |
|----------|---------------|------------|----------------|
| `postgres://` | SaveToPostgreSQL | `postgres://user:pass@host:5432/db` | `connection_string: <url>` |
| `mongo://` | SaveToMongoDB | `mongo://host:27017/database` | `connection_string: <url>` |
| `redis://` | SaveToRedis | `redis://host:6379/0` | `address: <host:port>`<br>`database: <db>` |
| `parquet://` | SaveToParquet | `parquet://path/to/files` | `path: <path>` |
| `kafka://` | SaveToKafka | `kafka://broker:9092/topic` | `brokers: [<broker>]`<br>`topic: <topic>` |

## Processor Shortcuts

### Built-in Aliases

| Shortcut | Full Type | Description |
|----------|-----------|-------------|
| `filter-payments` | FilterPayments | Filter payment operations |
| `latest-ledger` | LatestLedger | Extract latest ledger info |
| `contract-data` | ContractData | Process contract data |
| `account-data` | AccountData | Process account data |
| `transform-payment` | TransformToAppPayment | Transform to app payment format |
| `enrich-asset` | AssetEnrichment | Enrich asset information |

### Inline Filters

```bash
# Amount filters
filter amount > 100
filter amount >= 100 && amount < 1000

# Type filters
filter type == "payment"
filter operation_type in ["payment", "path_payment"]

# Time filters
filter created_at > "2024-01-01"
filter created_at > now() - 1h

# Complex filters
filter amount > 100 && (type == "payment" || type == "path_payment")
filter has:memo && memo.type == "text"
```

## Error Handling

### User-Friendly Error Messages

```bash
# Invalid syntax
$ flowctl run "stellar://mainnet | | postgres://db"
Error: Invalid pipeline syntax: empty processor at position 2

# Unknown protocol
$ flowctl run "unknown://source | postgres://db"
Error: Unknown protocol 'unknown'. Valid source protocols: stellar, s3, gs, file, kafka, http

# Missing consumer
$ flowctl run "stellar://mainnet | filter-payments"
Error: Pipeline must end with a consumer. Add a destination like:
  stellar://mainnet | filter-payments | postgres://database
  stellar://mainnet | filter-payments | stdout

# Invalid filter
$ flowctl run "stellar://mainnet | filter invalid syntax | stdout"
Error: Invalid filter expression 'invalid syntax'. Examples:
  filter amount > 100
  filter type == "payment"
  filter has:metadata
```

### Debugging Support

```bash
# Dry run to see generated config
$ flowctl run --dry-run "stellar://mainnet | filter-payments | stdout"
Generated v2 configuration:
source:
  stellar: mainnet
  
process:
  - payment_filter
  
save_to:
  - stdout

# Verbose mode for debugging
$ flowctl run --verbose "stellar://mainnet | filter-payments | stdout"
[DEBUG] Parsing pipeline expression...
[DEBUG] Resolved 'stellar://mainnet' to CaptiveCoreInboundAdapter
[DEBUG] Resolved 'filter-payments' to FilterPayments
[DEBUG] Generated v2 configuration
[INFO] Starting pipeline...
```

## Testing Strategy

### Unit Tests

```go
// parser_test.go
func TestParsePipeline(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected *Expression
        wantErr  bool
    }{
        {
            name:  "simple pipeline",
            input: "stellar://mainnet | filter-payments | postgres://db",
            expected: &Expression{
                Source: Component{Protocol: "stellar", Location: "mainnet"},
                Processors: []Component{{Raw: "filter-payments"}},
                Consumer: Component{Protocol: "postgres", Location: "db"},
            },
        },
        // More test cases...
    }
}
```

### Integration Tests

1. **Protocol Tests**: Verify each protocol handler correctly extracts configuration
2. **Alias Resolution**: Ensure all aliases from aliases.yaml work correctly
3. **End-to-End**: Run actual pipelines using one-liner syntax
4. **Error Cases**: Verify helpful error messages for common mistakes
5. **Performance**: Ensure parsing overhead is minimal (<10ms)

## Migration Guide

### When to Use One-Liners

**Good for**:
- Quick data exploration
- Simple ETL tasks
- Development and testing
- Demo and tutorials

**Not ideal for**:
- Complex multi-step processing
- Pipelines needing advanced configuration
- Production pipelines with monitoring
- Pipelines with custom processors

### Converting to Full Configuration

```bash
# Start with one-liner
$ flowctl run "stellar://mainnet | filter amount > 1000 | postgres://analytics"

# Convert to config file
$ flowctl convert "stellar://mainnet | filter amount > 1000 | postgres://analytics" > pipeline.yaml

# Generated pipeline.yaml:
source:
  stellar: mainnet
  
process:
  - filter_payments:
      min_amount: 1000
      
save_to:
  postgres: "${DATABASE_URL:-postgres://analytics}"
```

## Examples Gallery

### Common Use Cases

```bash
# 1. Archive Stellar payments to Parquet
flowctl run "stellar://mainnet | filter-payments | parquet://archive/payments"

# 2. Real-time contract monitoring
flowctl run "stellar://mainnet | contract-events | redis://events"

# 3. Account balance tracking
flowctl run "stellar://mainnet | account-data | mongo://accounts"

# 4. High-value payment alerts
flowctl run "stellar://mainnet | filter amount > 10000 | kafka://alerts"

# 5. Debug ledger processing
flowctl run "file:///data/ledgers.xdr | latest-ledger | stdout"

# 6. Cross-chain monitoring
flowctl run "kafka://blockchain-events | filter chain == 'stellar' | postgres://stellar_events"

# 7. Data lake ingestion
flowctl run "stellar://mainnet | transform-payment | gs://datalake/stellar/payments"
```

### Complex Examples

```bash
# Multi-step processing
flowctl run "stellar://mainnet | filter-payments | filter amount > 100 | transform-payment | postgres://warehouse"

# With parameters
flowctl run "s3://bucket/ledgers?region=us-west-2 | contract-data | parquet://output?compression=snappy"

# Multiple filters
flowctl run "stellar://mainnet | filter type == 'payment' && amount > 100 | postgres://filtered_payments"
```

## Future Extensions

### Phase 1: Enhanced Expressions
- Branching: `source | processor | (postgres://db1, redis://cache)`
- Conditional routing: `source | filter ? processor1 : processor2 | sink`
- Parallel processing: `source | [processor1, processor2] | sink`

### Phase 2: Interactive Mode
```bash
$ flowctl interactive
> source stellar://mainnet
✓ Connected to Stellar mainnet
> filter amount > 100
✓ Applied filter (matching 2,341 records)
> preview 5
✓ Showing 5 records...
> save postgres://analytics
✓ Pipeline saved and running
```

### Phase 3: Community Registry
```bash
# Use community processors
flowctl run "stellar://mainnet | @stellar/payment-classifier:v2 | postgres://classified"

# Share your processor
flowctl publish ./my-processor --name "@myorg/processor"
```

## Implementation Timeline

1. **Week 1**: Parser implementation and unit tests
2. **Week 2**: Protocol handlers and config builder
3. **Week 3**: CLI integration and error handling
4. **Week 4**: Integration tests and documentation
5. **Week 5**: Beta testing and refinements
6. **Week 6**: Release and user feedback

## Success Metrics

- Time from install to first pipeline: < 60 seconds
- Parser performance: < 10ms for typical expressions
- User success rate: > 90% of attempts result in running pipeline
- Error message helpfulness: > 80% of errors lead to successful retry
- Adoption: 50% of new users try one-liner syntax first