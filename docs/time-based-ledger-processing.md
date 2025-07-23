# Time-Based Ledger Processing Implementation Plan

## Overview

This document outlines the design and implementation plan for time-based ledger range processing in the CDP pipeline workflow. This feature allows users to specify relative time periods (e.g., "1 year back", "6 months ago") instead of explicit ledger sequences, enabling more intuitive configuration for time-sensitive data processing.

## Problem Statement

Currently, users must specify exact ledger sequences (`start_ledger` and `end_ledger`) in pipeline configurations. This approach has several limitations:

1. **Knowledge Barrier**: Users need to know the relationship between time and ledger sequences
2. **Manual Calculation**: Converting "data from last year" requires manual ledger sequence lookup
3. **Static Configuration**: Configurations become outdated as new ledgers are created
4. **Maintenance Overhead**: Long-running pipelines require manual updates to stay current

## Requirements

### Functional Requirements

1. **Relative Time Specification**: Support time-based configuration parameters
   - `start_time_ago`: Process data starting from X time ago (e.g., "1y", "6m", "30d")
   - `end_time_ago`: Process data until Y time ago (optional, defaults to "now")
   - `continuous_mode`: Continue processing new ledgers as they arrive (boolean)

2. **Time Format Support**: Accept multiple time formats
   - Duration strings: "1y", "6m", "30d", "12h", "45m"
   - ISO 8601 timestamps: "2024-01-01T00:00:00Z"
   - Relative phrases: "1 year ago", "6 months back"

3. **Dynamic Range Calculation**: Convert time periods to ledger sequences at runtime
   - Query Stellar network for current ledger information
   - Calculate historical ledger sequences based on average block time
   - Handle network-specific differences (testnet vs mainnet)

4. **Continuous Processing**: Support ongoing processing modes
   - Start from historical point and continue indefinitely
   - Stop at current time but resume when new ledgers arrive
   - Hybrid modes combining historical backfill with live processing

### Non-Functional Requirements

1. **Performance**: Time-to-ledger conversion should complete in <5 seconds
2. **Accuracy**: Ledger sequence calculations should be within ±10 ledgers of actual time
3. **Reliability**: Handle network failures gracefully with retry mechanisms
4. **Backward Compatibility**: Existing explicit ledger configurations must continue working

## Design Overview

### Configuration Schema

```yaml
# New time-based configuration options
source:
  type: BufferedStorageSourceAdapter
  config:
    bucket_name: "my-bucket"
    network: "testnet"
    
    # Time-based range specification (new)
    start_time_ago: "1y"          # Start processing 1 year ago
    end_time_ago: "1m"            # Stop processing 1 month ago (optional)
    continuous_mode: true         # Continue processing new ledgers
    
    # Alternative: absolute time specification (new)
    start_time: "2024-01-01T00:00:00Z"
    end_time: "2024-06-01T00:00:00Z"
    
    # Legacy configuration (still supported)
    start_ledger: 123456          # Explicit ledger numbers
    end_ledger: 789012
```

### Processing Modes

1. **Historical Range Mode**
   ```yaml
   start_time_ago: "1y"
   end_time_ago: "1m"
   continuous_mode: false
   ```
   Process data from 1 year ago until 1 month ago, then stop.

2. **Historical + Continuous Mode**
   ```yaml
   start_time_ago: "6m"
   continuous_mode: true
   ```
   Start processing 6 months ago and continue indefinitely with new ledgers.

3. **Live-Only Mode**
   ```yaml
   start_time_ago: "1h"
   end_time_ago: "now"
   continuous_mode: true
   ```
   Process recent data and continue with live updates.

## Implementation Plan

### Phase 1: Core Time Conversion Infrastructure

#### 1.1 Time Parser Module (`utils/timeparser.go`)

```go
type TimeSpecification struct {
    StartTimeAgo    string    `yaml:"start_time_ago,omitempty"`
    EndTimeAgo      string    `yaml:"end_time_ago,omitempty"`
    StartTime       time.Time `yaml:"start_time,omitempty"`
    EndTime         time.Time `yaml:"end_time,omitempty"`
    ContinuousMode  bool      `yaml:"continuous_mode,omitempty"`
}

type LedgerTimeConverter interface {
    ConvertTimeToLedger(ctx context.Context, targetTime time.Time, network string) (uint32, error)
    ConvertLedgerToTime(ctx context.Context, ledgerSeq uint32, network string) (time.Time, error)
    GetCurrentLedger(ctx context.Context, network string) (uint32, time.Time, error)
}
```

#### 1.2 Stellar Network Client (`utils/stellarnetwork.go`)

```go
type StellarNetworkClient struct {
    horizonURL string
    client     *horizonclient.Client
}

// Methods for querying network state
func (c *StellarNetworkClient) GetCurrentLedger(ctx context.Context) (*LedgerInfo, error)
func (c *StellarNetworkClient) GetLedgerBySequence(ctx context.Context, seq uint32) (*LedgerInfo, error)
func (c *StellarNetworkClient) EstimateLedgerAtTime(ctx context.Context, targetTime time.Time) (uint32, error)
```

#### 1.3 Time-to-Ledger Conversion Algorithm

**Strategy 1: Network Query Approach**
- Query Horizon API for recent ledgers with timestamps
- Calculate average ledger time based on recent samples
- Extrapolate backward to estimate historical ledger sequences

**Strategy 2: Genesis + Block Time Approach**
- Use known network genesis time and average block time
- Calculate estimated ledger sequences using linear interpolation
- More accurate for recent data, less accurate for historical data

**Strategy 3: Hybrid Approach (Recommended)**
- Use network queries for recent data (last 30 days)
- Fall back to genesis calculation for older historical data
- Cache results to improve performance

### Phase 2: Source Adapter Integration

#### 2.1 Enhanced Configuration Parsing

Update all source adapters to support time-based configuration:

```go
type EnhancedSourceConfig struct {
    // Legacy fields (preserved)
    StartLedger uint32 `yaml:"start_ledger,omitempty"`
    EndLedger   uint32 `yaml:"end_ledger,omitempty"`
    
    // New time-based fields
    TimeSpec    TimeSpecification `yaml:",inline"`
    
    // Resolved fields (calculated at runtime)
    ResolvedStartLedger uint32
    ResolvedEndLedger   uint32
}
```

#### 2.2 Runtime Ledger Resolution

```go
func ResolveTimeBasedLedgers(ctx context.Context, config *EnhancedSourceConfig, network string) error {
    converter := NewLedgerTimeConverter(network)
    
    // Handle explicit ledger sequences (legacy mode)
    if config.StartLedger > 0 {
        config.ResolvedStartLedger = config.StartLedger
        config.ResolvedEndLedger = config.EndLedger
        return nil
    }
    
    // Handle time-based specifications
    if config.TimeSpec.StartTimeAgo != "" {
        startTime := time.Now().Add(-parseDuration(config.TimeSpec.StartTimeAgo))
        startLedger, err := converter.ConvertTimeToLedger(ctx, startTime, network)
        if err != nil {
            return err
        }
        config.ResolvedStartLedger = startLedger
    }
    
    // Handle end time specification
    if config.TimeSpec.EndTimeAgo != "" && config.TimeSpec.EndTimeAgo != "now" {
        endTime := time.Now().Add(-parseDuration(config.TimeSpec.EndTimeAgo))
        endLedger, err := converter.ConvertTimeToLedger(ctx, endTime, network)
        if err != nil {
            return err
        }
        config.ResolvedEndLedger = endLedger
    }
    
    return nil
}
```

#### 2.3 Continuous Mode Support

```go
type ContinuousProcessor struct {
    baseRange       ledgerbackend.Range
    continuousMode  bool
    lastProcessed   uint32
    pollInterval    time.Duration
    networkClient   StellarNetworkClient
}

func (cp *ContinuousProcessor) Run(ctx context.Context) error {
    // Process historical range first
    if err := cp.processHistoricalRange(ctx); err != nil {
        return err
    }
    
    // If continuous mode, start polling for new ledgers
    if cp.continuousMode {
        return cp.processContinuousMode(ctx)
    }
    
    return nil
}
```

### Phase 3: Configuration Validation & Error Handling

#### 3.1 Configuration Validation

```go
func ValidateTimeBasedConfig(config *EnhancedSourceConfig) error {
    // Ensure mutual exclusivity
    if config.StartLedger > 0 && config.TimeSpec.StartTimeAgo != "" {
        return errors.New("cannot specify both start_ledger and start_time_ago")
    }
    
    // Validate time format
    if config.TimeSpec.StartTimeAgo != "" {
        if _, err := parseDuration(config.TimeSpec.StartTimeAgo); err != nil {
            return fmt.Errorf("invalid start_time_ago format: %w", err)
        }
    }
    
    // Validate time ranges
    if config.TimeSpec.StartTimeAgo != "" && config.TimeSpec.EndTimeAgo != "" {
        startDuration := parseDuration(config.TimeSpec.StartTimeAgo)
        endDuration := parseDuration(config.TimeSpec.EndTimeAgo)
        if startDuration < endDuration {
            return errors.New("start_time_ago must be further back than end_time_ago")
        }
    }
    
    return nil
}
```

#### 3.2 Error Handling & Fallbacks

```go
type TimeResolutionError struct {
    ConfigField string
    TimeValue   string
    Cause       error
}

func (e *TimeResolutionError) Error() string {
    return fmt.Sprintf("failed to resolve %s='%s': %v", e.ConfigField, e.TimeValue, e.Cause)
}

// Fallback strategies for network failures
func ResolveWithFallback(ctx context.Context, timeSpec string, network string) (uint32, error) {
    // Try primary network query
    if ledger, err := PrimaryTimeConversion(ctx, timeSpec, network); err == nil {
        return ledger, nil
    }
    
    // Fall back to genesis calculation
    if ledger, err := FallbackGenesisCalculation(timeSpec, network); err == nil {
        log.Printf("Warning: Using fallback calculation for %s", timeSpec)
        return ledger, nil
    }
    
    return 0, fmt.Errorf("all time resolution methods failed for %s", timeSpec)
}
```

### Phase 4: Performance Optimizations

#### 4.1 Caching Strategy

```go
type LedgerTimeCache struct {
    mu          sync.RWMutex
    entries     map[string]CacheEntry
    maxAge      time.Duration
}

type CacheEntry struct {
    LedgerSeq   uint32
    Timestamp   time.Time
    CachedAt    time.Time
}

// Cache recent ledger-to-time mappings to avoid repeated API calls
func (c *LedgerTimeCache) GetOrFetch(ctx context.Context, key string, fetchFn func() (uint32, time.Time, error)) (uint32, time.Time, error)
```

#### 4.2 Batch Network Queries

```go
// Query multiple ledgers in a single API call when possible
func (c *StellarNetworkClient) GetLedgerRange(ctx context.Context, startSeq, endSeq uint32) ([]LedgerInfo, error)

// Use binary search for efficient time-to-ledger conversion
func BinarySearchLedgerByTime(ctx context.Context, client StellarNetworkClient, targetTime time.Time, minSeq, maxSeq uint32) (uint32, error)
```

## Configuration Examples

### Example 1: Contract Data from Last 6 Months (Continuous)

```yaml
name: ContractDataLast6Months
source:
  type: BufferedStorageSourceAdapter
  config:
    bucket_name: "stellar-data-mainnet"
    network: "mainnet"
    start_time_ago: "6m"           # Start 6 months ago
    continuous_mode: true          # Continue with new ledgers
processors:
  - type: "contract_data"
    config:
      network_passphrase: "Public Global Stellar Network ; September 2015"
consumers:
  - type: "save_to_postgresql"
```

### Example 2: Historical Analysis (Specific Time Range)

```yaml
name: Q1AnalysisHistorical
source:
  type: BufferedStorageSourceAdapter
  config:
    bucket_name: "stellar-data-testnet"
    network: "testnet"
    start_time: "2024-01-01T00:00:00Z"
    end_time: "2024-03-31T23:59:59Z"
    continuous_mode: false         # Process only historical range
processors:
  - type: "contract_invocation"
consumers:
  - type: "save_to_zeromq"
```

### Example 3: Real-Time Processing with Short History

```yaml
name: RealtimeWithContext
source:
  type: BufferedStorageSourceAdapter
  config:
    bucket_name: "stellar-data-mainnet"
    network: "mainnet"
    start_time_ago: "1d"           # Include last day for context
    continuous_mode: true          # Continue processing live
processors:
  - type: "contract_data"
consumers:
  - type: "save_to_redis"
```

## Migration Strategy

### Backward Compatibility

1. **No Breaking Changes**: Existing configurations with explicit ledger sequences continue working
2. **Graceful Degradation**: If time-based resolution fails, provide clear error messages
3. **Documentation**: Update all examples to show both old and new configuration styles

### Migration Path

1. **Phase 1**: Implement time-based parsing alongside existing ledger parsing
2. **Phase 2**: Update documentation and examples to prefer time-based configuration
3. **Phase 3**: Add deprecation warnings for explicit ledger configurations (future release)
4. **Phase 4**: Consider removing explicit ledger support (major version bump)

## Testing Strategy

### Unit Tests

1. **Time Parsing**: Test various duration formats and edge cases
2. **Ledger Conversion**: Mock Stellar network responses for deterministic testing
3. **Configuration Validation**: Test all validation rules and error conditions

### Integration Tests

1. **Network Connectivity**: Test against real testnet/mainnet APIs
2. **Continuous Mode**: Verify proper handling of new ledger arrivals
3. **Historical Accuracy**: Compare calculated ledgers against known historical data

### Performance Tests

1. **Conversion Speed**: Measure time-to-ledger conversion performance
2. **Cache Efficiency**: Verify cache hit rates and memory usage
3. **Network Resilience**: Test behavior under network failures

## Security Considerations

1. **API Rate Limiting**: Respect Horizon API limits and implement backoff
2. **Input Validation**: Sanitize all time-based inputs to prevent injection
3. **Network Isolation**: Support private Horizon endpoints for sensitive environments
4. **Audit Logging**: Log all time-to-ledger conversions for audit trails

## Monitoring & Observability

1. **Metrics**: Track time conversion accuracy, cache performance, network latencies
2. **Alerts**: Monitor for time resolution failures and network connectivity issues
3. **Dashboards**: Display real-time pipeline progress and conversion statistics

## Future Enhancements

1. **Smart Caching**: Implement distributed cache for multi-instance deployments
2. **Time Zone Support**: Handle time zone specifications in configuration
3. **Custom Network Support**: Support private/custom Stellar networks
4. **Prediction**: Predict future ledger sequences for scheduling purposes
5. **Historical Accuracy**: Improve accuracy using checkpoint ledger data

## Success Metrics

1. **Usability**: 90% reduction in time-to-configuration for time-based use cases
2. **Accuracy**: <1% error rate in ledger sequence calculations
3. **Performance**: <5 second time-to-ledger resolution for any historical time
4. **Adoption**: 50% of new configurations use time-based parameters within 3 months