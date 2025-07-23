# Time-Bounded Processing Implementation Plan

## Overview
Enable pipelines to process from a fixed start ledger until a time-based end point (e.g., "until 10 days ago"), then stop.

## Use Case
```yaml
source:
  type: BufferedStorageSourceAdapter
  config:
    start_ledger: 575899           # Fixed historical start point
    end_time_ago: "10d"           # Process until 10 days ago, then stop
    network: "testnet"
```

## Current Implementation Status

### ✅ Already Implemented
- `end_time_ago` field parsing in `TimeSpecification` struct
- Time parsing utilities for various duration formats ("10d", "1mo", "2w", etc.)
- Hybrid time-to-ledger conversion infrastructure
- Enhanced configuration validation

### 🔄 Needs Updates

#### Phase 1: Mixed Configuration Support
**File**: `source_adapter_enhanced_config.go`
- **Current limitation**: Validation rejects mixed ledger/time configs
- **Required change**: Allow `start_ledger` + `end_time_ago` combination
- **Update validation logic**: 
  ```go
  // Allow mixed configurations
  hasStartLedger := c.StartLedger > 0
  hasEndTime := c.EndTimeAgo != "" || !c.EndTime.IsZero()
  hasMixedConfig := hasStartLedger && hasEndTime
  
  if hasMixedConfig {
      // Validate mixed config is logical
      return c.validateMixedConfig()
  }
  ```

#### Phase 2: Resolution Logic Enhancement  
**File**: `utils/ledgerconverter.go` - `ResolveTimeBasedLedgers` function
- **Current**: Processes full time-based or full ledger-based configs
- **Required**: Handle hybrid configurations where start is ledger, end is time
- **Logic**:
  ```go
  func ResolveTimeBasedLedgers(ctx context.Context, network string, timeSpec *TimeSpecification, startLedger, endLedger uint32) (resolvedStart, resolvedEnd uint32, err error) {
      // If explicit start ledger provided, use it
      if startLedger > 0 {
          resolvedStart = startLedger
      } else {
          // Convert start time to ledger (existing logic)
      }
      
      // Handle end time resolution
      if timeSpec.EndTimeAgo != "" || !timeSpec.EndTime.IsZero() {
          endTime, err := resolveEndTime(timeSpec)
          if err != nil {
              return 0, 0, err
          }
          
          resolvedEnd, err = converter.ConvertTimeToLedger(endTime, network)
          if err != nil {
              return 0, 0, err
          }
      } else if endLedger > 0 {
          resolvedEnd = endLedger
      }
      
      return resolvedStart, resolvedEnd, nil
  }
  ```

#### Phase 3: Enhanced Adapter Logic Updates
**Files**: `source_adapter_s3_enhanced.go`, `source_adapter_buffered_enhanced.go`
- **Current**: Assumes either full legacy or full time-based mode
- **Required**: Support mixed mode processing
- **Changes**:
  ```go
  // In Run() method
  if adapter.enhancedConfig.IsTimeBased() || adapter.enhancedConfig.HasTimeBasedEnd() {
      log.Printf("Resolving time-based configuration...")
      if err := adapter.enhancedConfig.Resolve(ctx); err != nil {
          return fmt.Errorf("failed to resolve configuration: %w", err)
      }
  }
  ```

#### Phase 4: Boundary Validation
**File**: `source_adapter_enhanced_config.go`
- **Add method**: `HasTimeBasedEnd() bool`
- **Add method**: `validateMixedConfig() error`
- **Validation logic**:
  - Ensure start_ledger < resolved end_ledger
  - Ensure end time is not in the future
  - Warn if time range is very large

## Implementation Steps

### Step 1: Update Configuration Validation
```go
// In Validate() method
func (c *EnhancedSourceConfig) Validate() error {
    hasLegacyStart := c.StartLedger > 0
    hasTimeStart := c.StartTimeAgo != "" || !c.StartTime.IsZero()
    hasLegacyEnd := c.EndLedger > 0  
    hasTimeEnd := c.EndTimeAgo != "" || !c.EndTime.IsZero()
    
    // Allow these combinations:
    // 1. Full legacy: start_ledger + end_ledger
    // 2. Full time: start_time_ago + end_time_ago
    // 3. Mixed: start_ledger + end_time_ago
    
    if hasLegacyStart && hasTimeEnd {
        return c.validateMixedConfig()
    }
    
    // ... existing validation logic
}
```

### Step 2: Add Helper Methods
```go
func (c *EnhancedSourceConfig) HasTimeBasedEnd() bool {
    return c.EndTimeAgo != "" || !c.EndTime.IsZero()
}

func (c *EnhancedSourceConfig) IsMixedMode() bool {
    return c.StartLedger > 0 && c.HasTimeBasedEnd()
}

func (c *EnhancedSourceConfig) validateMixedConfig() error {
    if c.StartLedger == 0 {
        return fmt.Errorf("start_ledger must be specified in mixed mode")
    }
    
    if c.EndTimeAgo == "" && c.EndTime.IsZero() {
        return fmt.Errorf("end time must be specified in mixed mode")
    }
    
    return nil
}
```

### Step 3: Update Resolution Logic
Modify `ResolveTimeBasedLedgers` to handle the case where:
- Input: `startLedger=575899`, `timeSpec.EndTimeAgo="10d"`
- Output: `resolvedStart=575899`, `resolvedEnd=61500000` (calculated from 10d ago)

### Step 4: Testing
Create test configuration:
```yaml
source:
  type: BufferedStorageSourceAdapter  
  config:
    start_ledger: 575899
    end_time_ago: "10d"
    network: "testnet"
```

Expected behavior:
- Start at ledger 575899
- Calculate ledger for 10 days ago (e.g., 61500000)
- Process ledgers 575899 → 61500000
- Stop when reaching end ledger

## Benefits
- Process historical data up to a specific time point
- Useful for analysis that needs "data until X days ago"
- Combines precision of ledger sequences with convenience of time specifications
- Maintains backward compatibility with existing configurations

## Timeline
- **Phase 1-2**: Configuration and resolution logic (2 hours)
- **Phase 3**: Adapter updates (1 hour) 
- **Phase 4**: Testing and validation (1 hour)
- **Total**: ~4 hours implementation time