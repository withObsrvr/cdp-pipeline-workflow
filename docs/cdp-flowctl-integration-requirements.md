# CDP Pipeline - Flowctl Integration Requirements

## Summary

The CDP pipeline already has basic flowctl integration (control plane client, registration, heartbeat). However, it's missing the stats collection mechanism that would provide observability data to flowctl. This document outlines the minimal implementation needed.

## What Needs to Be Implemented

### 1. Stats Collection Mechanism

Create a stats collector that periodically gathers metrics from all pipeline components:

```go
// pkg/control/collector.go
package control

import (
    "context"
    "time"
)

// StatsCollector periodically collects stats from pipeline components
type StatsCollector struct {
    stats    *PipelineStats
    interval time.Duration
}

func NewStatsCollector(stats *PipelineStats, interval time.Duration) *StatsCollector {
    return &StatsCollector{
        stats:    stats,
        interval: interval,
    }
}

func (c *StatsCollector) CollectFromComponents(ctx context.Context, components []StatsProvider) {
    ticker := time.NewTicker(c.interval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            for _, component := range components {
                if stats := component.GetStats(); stats.ComponentName != "" {
                    c.stats.UpdateComponentStats(stats)
                }
            }
        }
    }
}
```

### 2. StatsProvider Implementation for Sources

Add stats tracking to source adapters:

```go
// Example for BufferedStorageSourceAdapter
type BufferedStorageSourceAdapter struct {
    // existing fields...
    
    // Stats tracking
    stats struct {
        ledgersProcessed uint64
        bytesRead        uint64
        errors           uint64
        lastProcessedAt  time.Time
    }
}

func (a *BufferedStorageSourceAdapter) GetStats() ComponentStats {
    return ComponentStats{
        ComponentType: "source",
        ComponentName: "BufferedStorageSource",
        Stats: map[string]interface{}{
            "ledgers_processed": atomic.LoadUint64(&a.stats.ledgersProcessed),
            "bytes_read":        atomic.LoadUint64(&a.stats.bytesRead),
            "errors":            atomic.LoadUint64(&a.stats.errors),
            "buffer_size":       a.config.BufferSize,
            "num_workers":       a.config.NumWorkers,
        },
        LastUpdated: time.Now(),
    }
}

// Update stats during processing
func (a *BufferedStorageSourceAdapter) processLedger(ledger LedgerData) error {
    // existing processing...
    
    atomic.AddUint64(&a.stats.ledgersProcessed, 1)
    atomic.AddUint64(&a.stats.bytesRead, uint64(len(ledger.Data)))
    a.stats.lastProcessedAt = time.Now()
    
    return nil
}
```

### 3. StatsProvider Implementation for Processors

Add stats to processors (extending the existing example):

```go
// Example for FilterPayments processor
type FilterPayments struct {
    // existing fields...
    
    stats struct {
        processed uint64
        filtered  uint64
        passed    uint64
        errors    uint64
    }
}

func (p *FilterPayments) GetStats() ComponentStats {
    return ComponentStats{
        ComponentType: "processor",
        ComponentName: "FilterPayments",
        Stats: map[string]interface{}{
            "messages_processed": atomic.LoadUint64(&p.stats.processed),
            "messages_filtered":  atomic.LoadUint64(&p.stats.filtered),
            "messages_passed":    atomic.LoadUint64(&p.stats.passed),
            "errors":             atomic.LoadUint64(&p.stats.errors),
            "filter_ratio":       float64(p.stats.filtered) / float64(p.stats.processed),
        },
        LastUpdated: time.Now(),
    }
}
```

### 4. StatsProvider Implementation for Consumers

Add stats to consumers:

```go
// Example for SaveToPostgreSQL consumer
type SaveToPostgreSQL struct {
    // existing fields...
    
    stats struct {
        rowsInserted   uint64
        batchesWritten uint64
        writeLatencyMs uint64
        errors         uint64
    }
}

func (c *SaveToPostgreSQL) GetStats() ComponentStats {
    return ComponentStats{
        ComponentType: "consumer",
        ComponentName: "SaveToPostgreSQL",
        Stats: map[string]interface{}{
            "rows_inserted":    atomic.LoadUint64(&c.stats.rowsInserted),
            "batches_written":  atomic.LoadUint64(&c.stats.batchesWritten),
            "avg_latency_ms":   atomic.LoadUint64(&c.stats.writeLatencyMs) / atomic.LoadUint64(&c.stats.batchesWritten),
            "errors":           atomic.LoadUint64(&c.stats.errors),
            "connection_pool":  c.db.Stats().OpenConnections,
        },
        LastUpdated: time.Now(),
    }
}
```

### 5. Wire Up Stats Collection in main.go

Modify the `setupPipeline` function to start stats collection:

```go
func setupPipeline(ctx context.Context, pipelineConfig PipelineConfig) error {
    // ... existing setup code ...
    
    // After creating all components, start stats collection
    if controlClient != nil && pipelineStats != nil {
        // Collect all components that implement StatsProvider
        var statsProviders []control.StatsProvider
        
        // Add source if it implements StatsProvider
        if sp, ok := source.(control.StatsProvider); ok {
            statsProviders = append(statsProviders, sp)
        }
        
        // Add processors
        for _, proc := range processors {
            if sp, ok := proc.(control.StatsProvider); ok {
                statsProviders = append(statsProviders, sp)
            }
        }
        
        // Add consumers
        for _, cons := range consumers {
            if sp, ok := cons.(control.StatsProvider); ok {
                statsProviders = append(statsProviders, sp)
            }
        }
        
        // Start stats collection
        collector := control.NewStatsCollector(pipelineStats, 10*time.Second)
        go collector.CollectFromComponents(ctx, statsProviders)
    }
    
    // ... rest of the function ...
}
```

### 6. Optional: Base Types with Stats

To reduce boilerplate, create base types with built-in stats:

```go
// pkg/common/base/stats.go
type BaseProcessor struct {
    stats ProcessorStats
}

type ProcessorStats struct {
    Processed uint64
    Errors    uint64
    Latency   *LatencyTracker
}

func (b *BaseProcessor) RecordProcessed() {
    atomic.AddUint64(&b.stats.Processed, 1)
}

func (b *BaseProcessor) RecordError() {
    atomic.AddUint64(&b.stats.Errors, 1)
}

func (b *BaseProcessor) GetBaseStats() map[string]interface{} {
    return map[string]interface{}{
        "processed": atomic.LoadUint64(&b.stats.Processed),
        "errors":    atomic.LoadUint64(&b.stats.Errors),
        "latency_p50": b.stats.Latency.P50(),
        "latency_p99": b.stats.Latency.P99(),
    }
}
```

## Implementation Priority

1. **High Priority** (Week 1):
   - Implement stats collection mechanism
   - Add stats to 3-5 most used components (BufferedStorageSource, ContractEvent, SaveToPostgreSQL)
   - Wire up in main.go

2. **Medium Priority** (Week 2):
   - Add stats to remaining processors
   - Add stats to remaining consumers
   - Create base types for common stats

3. **Low Priority** (Week 3+):
   - Advanced metrics (histograms, percentiles)
   - Custom metrics per component type
   - Performance optimization of stats collection

## Benefits

With these minimal changes:
- Flowctl gets full visibility into CDP pipeline performance
- AI agents can analyze and optimize pipelines based on real metrics
- Operators can monitor pipeline health through flowctl dashboards
- Cost estimation becomes more accurate with actual resource usage data

## Testing

```go
// pkg/control/collector_test.go
func TestStatsCollection(t *testing.T) {
    // Create mock components
    source := &MockSource{name: "test-source"}
    processor := &MockProcessor{name: "test-processor"}
    
    // Create pipeline stats
    stats := NewPipelineStats("test-pipeline")
    
    // Create collector
    collector := NewStatsCollector(stats, 100*time.Millisecond)
    
    // Start collection
    ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
    defer cancel()
    
    go collector.CollectFromComponents(ctx, []StatsProvider{source, processor})
    
    // Wait for collection
    time.Sleep(500 * time.Millisecond)
    
    // Verify stats were collected
    metrics := stats.GetMetrics()
    assert.Greater(t, metrics["source.test-source.processed"], 0.0)
    assert.Greater(t, metrics["processor.test-processor.processed"], 0.0)
}
```

This minimal implementation provides flowctl with the observability it needs while keeping changes focused and manageable.