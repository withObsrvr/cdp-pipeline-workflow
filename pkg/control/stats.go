package control

import (
    "fmt"
    "os"
    "strconv"
    "sync"
    "time"
)

// StatsProvider is implemented by components that track statistics
type StatsProvider interface {
    GetStats() ComponentStats
}

// getHealthCheckTimeout returns the health check timeout duration.
// It reads from HEALTH_CHECK_TIMEOUT_SECONDS env var, defaulting to 30 seconds.
func getHealthCheckTimeout() time.Duration {
    if timeoutStr := os.Getenv("HEALTH_CHECK_TIMEOUT_SECONDS"); timeoutStr != "" {
        if seconds, err := strconv.Atoi(timeoutStr); err == nil && seconds > 0 {
            return time.Duration(seconds) * time.Second
        }
    }
    return 30 * time.Second
}

// ComponentStats represents statistics from a single component
type ComponentStats struct {
    ComponentType string
    ComponentName string
    Stats         map[string]interface{}
    LastUpdated   time.Time
}

// PipelineStats aggregates stats from all components
type PipelineStats struct {
    mu         sync.RWMutex
    PipelineID string
    StartTime  time.Time
    Components []ComponentStats
}

func NewPipelineStats(pipelineID string) *PipelineStats {
    return &PipelineStats{
        PipelineID: pipelineID,
        StartTime:  time.Now(),
        Components: make([]ComponentStats, 0),
    }
}

func (ps *PipelineStats) UpdateComponentStats(stats ComponentStats) {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    
    // Update existing or append new
    found := false
    for i, cs := range ps.Components {
        if cs.ComponentName == stats.ComponentName {
            ps.Components[i] = stats
            found = true
            break
        }
    }
    
    if !found {
        ps.Components = append(ps.Components, stats)
    }
}

func (ps *PipelineStats) GetMetrics() map[string]float64 {
    ps.mu.RLock()
    defer ps.mu.RUnlock()
    
    metrics := make(map[string]float64)
    
    // Pipeline-level metrics
    metrics["pipeline.uptime_seconds"] = time.Since(ps.StartTime).Seconds()
    metrics["pipeline.component_count"] = float64(len(ps.Components))
    
    // Component-level metrics
    for _, comp := range ps.Components {
        prefix := comp.ComponentType + "." + comp.ComponentName
        for key, value := range comp.Stats {
            if v, ok := value.(float64); ok {
                metrics[prefix+"."+key] = v
            } else if v, ok := value.(int); ok {
                metrics[prefix+"."+key] = float64(v)
            } else if v, ok := value.(int64); ok {
                metrics[prefix+"."+key] = float64(v)
            } else if v, ok := value.(uint32); ok {
                metrics[prefix+"."+key] = float64(v)
            } else if v, ok := value.(uint64); ok {
                metrics[prefix+"."+key] = float64(v)
            }
        }
    }
    
    return metrics
}

func (ps *PipelineStats) IsHealthy() bool {
    ps.mu.RLock()
    defer ps.mu.RUnlock()
    
    timeout := getHealthCheckTimeout()
    
    // Pipeline is healthy if all components reported stats recently
    for _, comp := range ps.Components {
        if time.Since(comp.LastUpdated) > timeout {
            return false
        }
    }
    
    return len(ps.Components) > 0
}

func (ps *PipelineStats) GetHealthDetails() map[string]string {
    ps.mu.RLock()
    defer ps.mu.RUnlock()
    
    timeout := getHealthCheckTimeout()
    
    details := make(map[string]string)
    details["pipeline_id"] = ps.PipelineID
    details["uptime"] = time.Since(ps.StartTime).String()
    details["component_count"] = fmt.Sprintf("%d", len(ps.Components))
    details["health_check_timeout"] = timeout.String()
    
    for _, comp := range ps.Components {
        status := "healthy"
        if time.Since(comp.LastUpdated) > timeout {
            status = "stale"
        }
        details[comp.ComponentName+"_status"] = status
    }
    
    return details
}