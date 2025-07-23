package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/withObsrvr/cdp-pipeline-workflow/utils"
)

// EnhancedSourceConfig represents configuration with time-based support
type EnhancedSourceConfig struct {
	// Legacy fields (preserved for backward compatibility)
	StartLedger uint32 `yaml:"start_ledger,omitempty" json:"start_ledger,omitempty"`
	EndLedger   uint32 `yaml:"end_ledger,omitempty" json:"end_ledger,omitempty"`

	// Time-based specification (embedded)
	utils.TimeSpecification `yaml:",inline"`

	// Resolved fields (calculated at runtime)
	ResolvedStartLedger uint32 `json:"resolved_start_ledger,omitempty"`
	ResolvedEndLedger   uint32 `json:"resolved_end_ledger,omitempty"`

	// Rolling window support for time-based end points
	RollingWindow        bool      `json:"rolling_window,omitempty"`
	LastEndLedgerUpdate  time.Time `json:"last_end_ledger_update,omitempty"`
	
	// Network information
	Network string `yaml:"network" json:"network"`
}

// ParseEnhancedConfig parses configuration with time-based support
func ParseEnhancedConfig(config map[string]interface{}) (*EnhancedSourceConfig, error) {
	enhanced := &EnhancedSourceConfig{}

	// Parse network (required)
	if network, ok := config["network"].(string); ok {
		enhanced.Network = network
	} else {
		return nil, fmt.Errorf("network must be specified")
	}

	// Parse legacy ledger fields
	if startLedger, ok := getIntValue(config["start_ledger"]); ok && startLedger > 0 {
		enhanced.StartLedger = uint32(startLedger)
	}

	if endLedger, ok := getIntValue(config["end_ledger"]); ok && endLedger > 0 {
		enhanced.EndLedger = uint32(endLedger)
	}

	// Parse time-based fields
	if startTimeAgo, ok := config["start_time_ago"].(string); ok {
		enhanced.StartTimeAgo = startTimeAgo
	}

	if endTimeAgo, ok := config["end_time_ago"].(string); ok {
		enhanced.EndTimeAgo = endTimeAgo
	}

	// Parse absolute time fields
	if startTimeStr, ok := config["start_time"].(string); ok {
		startTime, err := time.Parse(time.RFC3339, startTimeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid start_time format: %w", err)
		}
		enhanced.StartTime = startTime
	}

	if endTimeStr, ok := config["end_time"].(string); ok {
		endTime, err := time.Parse(time.RFC3339, endTimeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid end_time format: %w", err)
		}
		enhanced.EndTime = endTime
	}

	// Parse continuous mode
	if continuousMode, ok := config["continuous_mode"].(bool); ok {
		enhanced.ContinuousMode = continuousMode
	}

	// Validate the configuration
	if err := enhanced.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return enhanced, nil
}

// Validate checks the configuration for logical consistency
func (c *EnhancedSourceConfig) Validate() error {
	// Check for different configuration types
	hasLegacyStart := c.StartLedger > 0
	hasTimeStart := c.StartTimeAgo != "" || !c.StartTime.IsZero()
	hasLegacyEnd := c.EndLedger > 0
	hasTimeEnd := c.EndTimeAgo != "" || !c.EndTime.IsZero()
	
	// Determine configuration mode
	isFullLegacy := hasLegacyStart && hasLegacyEnd && !hasTimeStart && !hasTimeEnd
	isFullTime := hasTimeStart && (hasTimeEnd || c.ContinuousMode) && !hasLegacyStart && !hasLegacyEnd
	isMixedMode := hasLegacyStart && hasTimeEnd && !hasTimeStart && !hasLegacyEnd
	
	// Validate supported combinations
	if !isFullLegacy && !isFullTime && !isMixedMode {
		return fmt.Errorf("unsupported configuration combination. Supported modes: " +
			"1) start_ledger + end_ledger, " +
			"2) start_time_ago + (end_time_ago OR continuous_mode), " +
			"3) start_ledger + end_time_ago")
	}

	// Validate mixed mode
	if isMixedMode {
		return c.validateMixedConfig()
	}

	// Validate time specification if present
	if isFullTime || isMixedMode {
		if err := utils.ValidateTimeSpecification(&c.TimeSpecification); err != nil {
			return err
		}
	}

	// Validate legacy configuration
	if isFullLegacy {
		if c.EndLedger > 0 && c.EndLedger < c.StartLedger {
			return fmt.Errorf("end_ledger must be greater than or equal to start_ledger")
		}
	}

	return nil
}

// Resolve converts time-based configuration to ledger sequences
func (c *EnhancedSourceConfig) Resolve(ctx context.Context) error {
	// If already resolved, return (unless rolling window needs update)
	if c.ResolvedStartLedger > 0 && !c.shouldUpdateEndLedger() {
		return nil
	}

	// Use the utility function to resolve
	resolvedStart, resolvedEnd, err := utils.ResolveTimeBasedLedgers(
		ctx,
		c.Network,
		&c.TimeSpecification,
		c.StartLedger,
		c.EndLedger,
	)
	if err != nil {
		return fmt.Errorf("failed to resolve time-based configuration: %w", err)
	}

	c.ResolvedStartLedger = resolvedStart
	c.ResolvedEndLedger = resolvedEnd
	
	// Set up rolling window if using time-based end
	if c.HasTimeBasedEnd() {
		c.RollingWindow = true
		c.LastEndLedgerUpdate = time.Now()
	}

	// Log the resolution
	if c.StartTimeAgo != "" || !c.StartTime.IsZero() || c.RollingWindow {
		log.Printf("Resolved time-based configuration: start_ledger=%d, end_ledger=%d, rolling=%v",
			c.ResolvedStartLedger, c.ResolvedEndLedger, c.RollingWindow)
	}

	return nil
}

// GetLedgerRange returns the resolved ledger range
func (c *EnhancedSourceConfig) GetLedgerRange() (startLedger, endLedger uint32) {
	if c.ResolvedStartLedger > 0 {
		return c.ResolvedStartLedger, c.ResolvedEndLedger
	}
	return c.StartLedger, c.EndLedger
}

// IsTimeBased returns true if this is a time-based configuration
func (c *EnhancedSourceConfig) IsTimeBased() bool {
	return c.StartTimeAgo != "" || c.EndTimeAgo != "" || !c.StartTime.IsZero() || !c.EndTime.IsZero()
}

// HasTimeBasedEnd returns true if the end point is time-based
func (c *EnhancedSourceConfig) HasTimeBasedEnd() bool {
	return c.EndTimeAgo != "" || !c.EndTime.IsZero()
}

// IsMixedMode returns true if this uses mixed ledger/time configuration
func (c *EnhancedSourceConfig) IsMixedMode() bool {
	hasLegacyStart := c.StartLedger > 0
	hasTimeEnd := c.HasTimeBasedEnd()
	hasTimeStart := c.StartTimeAgo != "" || !c.StartTime.IsZero()
	hasLegacyEnd := c.EndLedger > 0
	
	return hasLegacyStart && hasTimeEnd && !hasTimeStart && !hasLegacyEnd
}

// IsContinuous returns true if continuous mode is enabled
func (c *EnhancedSourceConfig) IsContinuous() bool {
	return c.ContinuousMode
}

// IsMixedContinuous returns true if this uses mixed mode with continuous processing
func (c *EnhancedSourceConfig) IsMixedContinuous() bool {
	return c.IsMixedMode() && c.ContinuousMode
}

// validateMixedConfig validates mixed mode configuration
func (c *EnhancedSourceConfig) validateMixedConfig() error {
	if c.StartLedger == 0 {
		return fmt.Errorf("start_ledger must be specified in mixed mode")
	}
	
	if !c.HasTimeBasedEnd() {
		return fmt.Errorf("end time must be specified in mixed mode (end_time_ago or end_time)")
	}
	
	// Continuous mode is now allowed in mixed mode
	// It will process from start_ledger to end_time, then continue with new ledgers
	
	return nil
}

// Helper function to safely convert interface{} to int
func getIntValue(v interface{}) (int, bool) {
	switch i := v.(type) {
	case int:
		return i, true
	case float64:
		return int(i), true
	case int64:
		return int(i), true
	}
	return 0, false
}

// ContinuousLedgerProcessor handles continuous ledger processing
type ContinuousLedgerProcessor struct {
	config        *EnhancedSourceConfig
	lastProcessed uint32
	pollInterval  time.Duration
	converter     utils.LedgerTimeConverter
}

// NewContinuousLedgerProcessor creates a new continuous processor
func NewContinuousLedgerProcessor(config *EnhancedSourceConfig) (*ContinuousLedgerProcessor, error) {
	converter, err := utils.NewHybridLedgerTimeConverter(config.Network)
	if err != nil {
		return nil, fmt.Errorf("failed to create ledger converter: %w", err)
	}

	return &ContinuousLedgerProcessor{
		config:       config,
		pollInterval: 5 * time.Second, // Poll every 5 seconds
		converter:    converter,
	}, nil
}

// GetNextLedgerRange returns the next range of ledgers to process
func (p *ContinuousLedgerProcessor) GetNextLedgerRange(ctx context.Context) (start, end uint32, hasMore bool, err error) {
	// If this is the first call, return the initial resolved range
	if p.lastProcessed == 0 {
		start, end = p.config.GetLedgerRange()
		p.lastProcessed = end
		return start, end, true, nil
	}

	// Get current ledger from network
	currentLedger, _, err := p.converter.GetCurrentLedgerForNetwork(p.config.Network)
	if err != nil {
		return 0, 0, false, fmt.Errorf("failed to get current ledger: %w", err)
	}

	// Check if there are new ledgers to process
	if currentLedger <= p.lastProcessed {
		return 0, 0, false, nil // No new ledgers
	}

	// Return the new range
	start = p.lastProcessed + 1
	end = currentLedger
	p.lastProcessed = end

	return start, end, true, nil
}

// WaitForNewLedgers waits for new ledgers to become available
func (p *ContinuousLedgerProcessor) WaitForNewLedgers(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(p.pollInterval):
		return nil
	}
}

// shouldUpdateEndLedger determines if the end ledger needs to be recalculated
func (c *EnhancedSourceConfig) shouldUpdateEndLedger() bool {
	if !c.RollingWindow {
		return false
	}
	
	// Update every 30 seconds to keep the rolling window fresh
	return time.Since(c.LastEndLedgerUpdate) > 30*time.Second
}

// IsRollingWindow returns true if this configuration uses a rolling time window
func (c *EnhancedSourceConfig) IsRollingWindow() bool {
	return c.RollingWindow
}

// GetCurrentEndLedger calculates the current end ledger for rolling windows
func (c *EnhancedSourceConfig) GetCurrentEndLedger(ctx context.Context) (uint32, error) {
	if !c.RollingWindow {
		return c.ResolvedEndLedger, nil
	}
	
	// For rolling windows, recalculate the end ledger based on current time
	converter, err := utils.NewHybridLedgerTimeConverter(c.Network)
	if err != nil {
		return 0, fmt.Errorf("failed to create converter: %w", err)
	}
	
	// Calculate the time threshold (e.g., "10 days ago" from now)
	var endTime time.Time
	if c.EndTimeAgo != "" {
		duration, err := utils.ParseDuration(c.EndTimeAgo)
		if err != nil {
			return 0, fmt.Errorf("failed to parse end_time_ago: %w", err)
		}
		endTime = time.Now().Add(-duration)
	} else if !c.EndTime.IsZero() {
		endTime = c.EndTime
	} else {
		return c.ResolvedEndLedger, nil
	}
	
	// Convert time to ledger
	endLedger, err := converter.ConvertTimeToLedger(endTime, c.Network)
	if err != nil {
		return 0, fmt.Errorf("failed to convert time to ledger: %w", err)
	}
	
	return endLedger, nil
}