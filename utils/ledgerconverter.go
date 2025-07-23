package utils

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// LedgerTimeConverter defines the interface for converting between ledger sequences and times
type LedgerTimeConverter interface {
	ConvertTimeToLedger(targetTime time.Time, network string) (uint32, error)
	ConvertLedgerToTime(ledgerSeq uint32, network string) (time.Time, error)
	GetCurrentLedgerForNetwork(network string) (uint32, time.Time, error)
}

// HybridLedgerTimeConverter implements the recommended hybrid approach for time-to-ledger conversion
type HybridLedgerTimeConverter struct {
	networkClient *StellarNetworkClient
	fallbackMode  bool
	cache         *conversionCache
	mu            sync.RWMutex
}

// conversionCache caches time-to-ledger conversions
type conversionCache struct {
	mu      sync.RWMutex
	entries map[string]*conversionEntry
	maxAge  time.Duration
}

type conversionEntry struct {
	ledgerSeq uint32
	timestamp time.Time
	cachedAt  time.Time
}

// NewHybridLedgerTimeConverter creates a new hybrid converter
func NewHybridLedgerTimeConverter(network string) (*HybridLedgerTimeConverter, error) {
	client, err := NewStellarNetworkClient(network)
	if err != nil {
		return nil, fmt.Errorf("failed to create network client: %w", err)
	}

	return &HybridLedgerTimeConverter{
		networkClient: client,
		fallbackMode:  false,
		cache: &conversionCache{
			entries: make(map[string]*conversionEntry),
			maxAge:  30 * time.Minute, // Cache conversions for 30 minutes
		},
	}, nil
}

// ConvertTimeToLedger converts a target time to the closest ledger sequence
func (h *HybridLedgerTimeConverter) ConvertTimeToLedger(targetTime time.Time, network string) (uint32, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("time:%d", targetTime.Unix())
	if cached := h.cache.get(cacheKey); cached != nil {
		return cached.ledgerSeq, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try network query approach first (more accurate for recent data)
	ledgerSeq, err := h.networkQueryApproach(ctx, targetTime)
	if err == nil {
		h.cache.set(cacheKey, ledgerSeq, targetTime)
		return ledgerSeq, nil
	}

	log.Printf("Network query failed, falling back to genesis calculation: %v", err)

	// Fall back to genesis calculation
	ledgerSeq = h.genesisCalculationApproach(targetTime)
	h.cache.set(cacheKey, ledgerSeq, targetTime)
	
	return ledgerSeq, nil
}

// ConvertLedgerToTime converts a ledger sequence to its close time
func (h *HybridLedgerTimeConverter) ConvertLedgerToTime(ledgerSeq uint32, network string) (time.Time, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("ledger:%d", ledgerSeq)
	if cached := h.cache.get(cacheKey); cached != nil {
		return cached.timestamp, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try to get exact time from network
	ledgerInfo, err := h.networkClient.GetLedgerBySequence(ctx, ledgerSeq)
	if err == nil {
		h.cache.set(cacheKey, ledgerSeq, ledgerInfo.CloseTime)
		return ledgerInfo.CloseTime, nil
	}

	log.Printf("Failed to get ledger %d from network, using estimation: %v", ledgerSeq, err)

	// Fall back to estimation
	estimatedTime := h.networkClient.estimateTimeFromGenesis(ledgerSeq)
	h.cache.set(cacheKey, ledgerSeq, estimatedTime)
	
	return estimatedTime, nil
}

// GetCurrentLedger gets the current ledger sequence and time
func (h *HybridLedgerTimeConverter) GetCurrentLedger(network string) (uint32, time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ledgerInfo, err := h.networkClient.GetCurrentLedger(ctx)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("failed to get current ledger: %w", err)
	}

	return ledgerInfo.Sequence, ledgerInfo.CloseTime, nil
}

// GetCurrentLedgerForNetwork implements the LedgerTimeConverter interface
func (h *HybridLedgerTimeConverter) GetCurrentLedgerForNetwork(network string) (uint32, time.Time, error) {
	return h.GetCurrentLedger(network)
}

// networkQueryApproach uses Horizon API to find the ledger at target time
func (h *HybridLedgerTimeConverter) networkQueryApproach(ctx context.Context, targetTime time.Time) (uint32, error) {
	// Get current ledger as reference point
	currentLedger, err := h.networkClient.GetCurrentLedger(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get current ledger: %w", err)
	}

	log.Printf("DEBUG: Network query approach - target: %v, current ledger time: %v", targetTime, currentLedger.CloseTime)

	// If target is in the future, return current ledger
	if targetTime.After(currentLedger.CloseTime) {
		log.Printf("DEBUG: Target time is in future, returning current ledger %d", currentLedger.Sequence)
		return currentLedger.Sequence, nil
	}

	// If target is very recent (within last 30 days), use binary search
	thirtyDaysAgo := time.Now().Add(-30 * 24 * time.Hour)
	log.Printf("DEBUG: Checking if target %v is after 30 days ago %v", targetTime, thirtyDaysAgo)
	if targetTime.After(thirtyDaysAgo) {
		log.Printf("DEBUG: Using binary search for recent data")
		return h.binarySearchRecent(ctx, targetTime, currentLedger)
	}

	// For older data, use estimation + refinement
	log.Printf("DEBUG: Using genesis estimation for older data")
	estimatedSeq := h.networkClient.estimateFromGenesis(targetTime)
	log.Printf("DEBUG: Genesis estimate: %d", estimatedSeq)
	return h.refineEstimate(ctx, targetTime, estimatedSeq)
}

// binarySearchRecent performs efficient binary search for recent ledgers
func (h *HybridLedgerTimeConverter) binarySearchRecent(ctx context.Context, targetTime time.Time, currentLedger *LedgerInfo) (uint32, error) {
	// Calculate approximate start point (30 days back)
	constants := networkConstants[h.networkClient.network]
	ledgersIn30Days := uint32(30 * 24 * 60 * 60 * 1000 / constants.AverageLedgerTimeMs)
	log.Printf("DEBUG: Binary search recent for network %s: current=%d, ledgersIn30Days=%d", 
		h.networkClient.network, currentLedger.Sequence, ledgersIn30Days)
	
	minSeq := currentLedger.Sequence - ledgersIn30Days
	if minSeq < MinAvailableLedger {
		minSeq = MinAvailableLedger
	}
	maxSeq := currentLedger.Sequence
	log.Printf("DEBUG: Binary search range: [%d, %d]", minSeq, maxSeq)

	result, err := h.binarySearch(ctx, targetTime, minSeq, maxSeq)
	log.Printf("DEBUG: Binary search result: %d (error: %v)", result, err)
	return result, err
}

// binarySearch performs binary search between min and max sequences
func (h *HybridLedgerTimeConverter) binarySearch(ctx context.Context, targetTime time.Time, minSeq, maxSeq uint32) (uint32, error) {
	maxIterations := 20
	iteration := 0

	for minSeq < maxSeq && iteration < maxIterations {
		iteration++
		
		if maxSeq-minSeq <= 1 {
			// Check both and return closest
			minLedger, err1 := h.networkClient.GetLedgerBySequence(ctx, minSeq)
			maxLedger, err2 := h.networkClient.GetLedgerBySequence(ctx, maxSeq)
			
			if err1 != nil && err2 != nil {
				return minSeq, fmt.Errorf("both ledgers not found")
			}
			
			if err1 == nil && err2 == nil {
				// Return the one closest to target time
				minDiff := absTimeDiff(minLedger.CloseTime, targetTime)
				maxDiff := absTimeDiff(maxLedger.CloseTime, targetTime)
				
				if minDiff < maxDiff {
					return minSeq, nil
				}
				return maxSeq, nil
			}
			
			if err1 == nil {
				return minSeq, nil
			}
			return maxSeq, nil
		}

		midSeq := (minSeq + maxSeq) / 2
		
		ledger, err := h.networkClient.GetLedgerBySequence(ctx, midSeq)
		if err != nil {
			// If ledger not found, try to narrow the range
			// This could happen if we're querying a pruned ledger
			if midSeq-minSeq > maxSeq-midSeq {
				maxSeq = midSeq - 1
			} else {
				minSeq = midSeq + 1
			}
			continue
		}

		if ledger.CloseTime.Before(targetTime) {
			minSeq = midSeq + 1
		} else if ledger.CloseTime.After(targetTime) {
			maxSeq = midSeq - 1
		} else {
			// Exact match
			return midSeq, nil
		}
	}

	// Return the best estimate
	return (minSeq + maxSeq) / 2, nil
}

// refineEstimate refines an estimated ledger sequence
func (h *HybridLedgerTimeConverter) refineEstimate(ctx context.Context, targetTime time.Time, estimate uint32) (uint32, error) {
	// Try to get the estimated ledger
	ledger, err := h.networkClient.GetLedgerBySequence(ctx, estimate)
	if err != nil {
		// If not found, return the estimate
		return estimate, nil
	}

	// Calculate the time difference
	timeDiff := ledger.CloseTime.Sub(targetTime)
	
	// If close enough (within 1 minute), return the estimate
	if absTimeDiff(ledger.CloseTime, targetTime) < time.Minute {
		return estimate, nil
	}

	// Adjust the estimate based on the time difference
	constants := networkConstants[h.networkClient.network]
	ledgerAdjustment := int64(timeDiff.Milliseconds() / constants.AverageLedgerTimeMs)
	
	newEstimate := int64(estimate) - ledgerAdjustment
	if newEstimate < 1 {
		newEstimate = 1
	}

	// Do a small binary search around the new estimate
	searchRadius := uint32(100)
	minSeq := uint32(newEstimate)
	if minSeq > searchRadius {
		minSeq = uint32(newEstimate) - searchRadius
	} else {
		minSeq = MinAvailableLedger
	}
	maxSeq := uint32(newEstimate) + searchRadius

	return h.binarySearch(ctx, targetTime, minSeq, maxSeq)
}

// genesisCalculationApproach uses genesis time and average block time
func (h *HybridLedgerTimeConverter) genesisCalculationApproach(targetTime time.Time) uint32 {
	return h.networkClient.estimateFromGenesis(targetTime)
}

// Helper function to calculate absolute time difference
func absTimeDiff(t1, t2 time.Time) time.Duration {
	diff := t1.Sub(t2)
	if diff < 0 {
		return -diff
	}
	return diff
}

// ResolveTimeBasedLedgers resolves time-based configuration to ledger sequences
func ResolveTimeBasedLedgers(ctx context.Context, network string, timeSpec *TimeSpecification, startLedger, endLedger uint32) (resolvedStart, resolvedEnd uint32, err error) {
	// Create converter for any time-based resolution
	log.Printf("DEBUG: Creating ledger time converter for network: %s", network)
	converter, err := NewHybridLedgerTimeConverter(network)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create ledger time converter: %w", err)
	}

	// Determine if we have any time-based configuration
	hasTimeStart := timeSpec.StartTimeAgo != "" || !timeSpec.StartTime.IsZero()
	hasTimeEnd := timeSpec.EndTimeAgo != "" || !timeSpec.EndTime.IsZero()
	
	// Handle mixed mode: start_ledger + end_time_ago
	if startLedger > 0 && hasTimeEnd && !hasTimeStart {
		log.Printf("Using mixed mode: start_ledger=%d with time-based end", startLedger)
		
		// Use provided start ledger
		resolvedStart = startLedger
		
		// Resolve end time specification
		now := time.Now()
		_, endTime, err := ResolveTimeSpecification(timeSpec, now)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to resolve end time specification: %w", err)
		}
		
		// Convert end time to ledger
		resolvedEnd, err = converter.ConvertTimeToLedger(endTime, network)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to convert end time to ledger: %w", err)
		}
		
		// Validate that end ledger is greater than start ledger
		if resolvedEnd <= startLedger {
			return 0, 0, fmt.Errorf("resolved end ledger (%d) must be greater than start ledger (%d)", resolvedEnd, startLedger)
		}
		
		log.Printf("Mixed mode resolved: start=%d, end=%d", resolvedStart, resolvedEnd)
		return resolvedStart, resolvedEnd, nil
	}

	// Handle full legacy mode: start_ledger + end_ledger
	if startLedger > 0 && endLedger > 0 && !hasTimeStart && !hasTimeEnd {
		return startLedger, endLedger, nil
	}

	// Handle full time-based mode
	if hasTimeStart {
		// Resolve time specification to absolute times
		now := time.Now()
		startTime, endTime, err := ResolveTimeSpecification(timeSpec, now)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to resolve time specification: %w", err)
		}

		// Convert start time to ledger
		if !startTime.IsZero() {
			log.Printf("DEBUG: Converting start time %v to ledger for network %s", startTime, network)
			resolvedStart, err = converter.ConvertTimeToLedger(startTime, network)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to convert start time to ledger: %w", err)
			}
			log.Printf("DEBUG: Start time %v converted to ledger %d", startTime, resolvedStart)
		}

		// Convert end time to ledger if specified and not continuous
		if !endTime.IsZero() && !timeSpec.ContinuousMode {
			// For continuous mode, we don't resolve end ledger
			if endTime.Before(now.Add(24 * time.Hour)) {
				// Only resolve if end time is not in far future
				resolvedEnd, err = converter.ConvertTimeToLedger(endTime, network)
				if err != nil {
					return 0, 0, fmt.Errorf("failed to convert end time to ledger: %w", err)
				}
			}
		}

		return resolvedStart, resolvedEnd, nil
	}

	return 0, 0, fmt.Errorf("unsupported configuration: no valid start point specified")
}

// Cache methods for conversionCache
func (c *conversionCache) get(key string) *conversionEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[key]
	if !ok {
		return nil
	}

	// Check if entry is expired
	if time.Since(entry.cachedAt) > c.maxAge {
		return nil
	}

	return entry
}

func (c *conversionCache) set(key string, ledgerSeq uint32, timestamp time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = &conversionEntry{
		ledgerSeq: ledgerSeq,
		timestamp: timestamp,
		cachedAt:  time.Now(),
	}

	// Simple cleanup
	if len(c.entries) > 5000 {
		c.cleanup()
	}
}

func (c *conversionCache) cleanup() {
	now := time.Now()
	for key, entry := range c.entries {
		if now.Sub(entry.cachedAt) > c.maxAge {
			delete(c.entries, key)
		}
	}
}