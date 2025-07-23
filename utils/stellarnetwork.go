package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

// NetworkConstants holds network-specific constants
type NetworkConstants struct {
	GenesisTime         time.Time
	AverageLedgerTimeMs int64
	HorizonURL          string
}

var (
	networkConstants = map[string]NetworkConstants{
		"pubnet": {
			GenesisTime:         time.Date(2015, 9, 30, 16, 46, 54, 0, time.UTC), // Stellar mainnet genesis
			AverageLedgerTimeMs: 5000, // ~5 seconds per ledger
			HorizonURL:          "https://horizon.stellar.org",
		},
		"testnet": {
			GenesisTime:         time.Date(2015, 9, 30, 16, 46, 54, 0, time.UTC), // Using same as mainnet for consistency
			AverageLedgerTimeMs: 5000, // ~5 seconds per ledger
			HorizonURL:          "https://horizon-testnet.stellar.org",
		},
	}
)

// LedgerInfo contains information about a ledger
type LedgerInfo struct {
	Sequence  uint32    `json:"sequence"`
	Hash      string    `json:"hash"`
	CloseTime time.Time `json:"closed_at"`
}

// HorizonLedgerResponse represents the Horizon API response for a ledger
type HorizonLedgerResponse struct {
	Sequence            uint32 `json:"sequence"`
	Hash                string `json:"hash"`
	ClosedAt            string `json:"closed_at"`
	SuccessfulTransactionCount int32  `json:"successful_transaction_count"`
	FailedTransactionCount     int32  `json:"failed_transaction_count"`
}

// StellarNetworkClient provides methods for interacting with the Stellar network
type StellarNetworkClient struct {
	network    string
	horizonURL string
	httpClient *http.Client
	cache      *ledgerCache
}

// ledgerCache provides thread-safe caching for ledger information
type ledgerCache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	maxAge  time.Duration
}

type cacheEntry struct {
	ledgerInfo *LedgerInfo
	cachedAt   time.Time
}

// NewStellarNetworkClient creates a new Stellar network client
func NewStellarNetworkClient(network string) (*StellarNetworkClient, error) {
	log.Printf("DEBUG: Creating StellarNetworkClient for network: %s", network)
	constants, ok := networkConstants[network]
	if !ok {
		return nil, fmt.Errorf("unsupported network: %s", network)
	}

	log.Printf("DEBUG: Using Horizon URL: %s for network: %s", constants.HorizonURL, network)

	client := &StellarNetworkClient{
		network:    network,
		horizonURL: constants.HorizonURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		cache: &ledgerCache{
			entries: make(map[string]*cacheEntry),
			maxAge:  5 * time.Minute,
		},
	}

	return client, nil
}

// GetCurrentLedger returns information about the current ledger
func (c *StellarNetworkClient) GetCurrentLedger(ctx context.Context) (*LedgerInfo, error) {
	// Check cache first
	cacheKey := "current"
	if cached := c.cache.get(cacheKey); cached != nil {
		return cached, nil
	}

	// Query Horizon for the latest ledger
	url := fmt.Sprintf("%s/ledgers?order=desc&limit=1", c.horizonURL)
	log.Printf("DEBUG: Querying Horizon URL: %s (network: %s)", url, c.network)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query Horizon: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Horizon returned status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		Embedded struct {
			Records []HorizonLedgerResponse `json:"records"`
		} `json:"_embedded"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if len(response.Embedded.Records) == 0 {
		return nil, fmt.Errorf("no ledgers found")
	}

	ledger := response.Embedded.Records[0]
	closeTime, err := time.Parse(time.RFC3339, ledger.ClosedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse close time: %w", err)
	}

	info := &LedgerInfo{
		Sequence:  ledger.Sequence,
		Hash:      ledger.Hash,
		CloseTime: closeTime,
	}

	log.Printf("DEBUG: Retrieved current ledger %d from %s network (URL: %s)", ledger.Sequence, c.network, c.horizonURL)

	// Cache the result with short TTL for current ledger
	c.cache.set(cacheKey, info, 10*time.Second)

	return info, nil
}

// GetLedgerBySequence returns information about a specific ledger
func (c *StellarNetworkClient) GetLedgerBySequence(ctx context.Context, sequence uint32) (*LedgerInfo, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("seq:%d", sequence)
	if cached := c.cache.get(cacheKey); cached != nil {
		return cached, nil
	}

	url := fmt.Sprintf("%s/ledgers/%d", c.horizonURL, sequence)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query Horizon: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("ledger %d not found", sequence)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Horizon returned status %d: %s", resp.StatusCode, string(body))
	}

	var ledger HorizonLedgerResponse
	if err := json.NewDecoder(resp.Body).Decode(&ledger); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	closeTime, err := time.Parse(time.RFC3339, ledger.ClosedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse close time: %w", err)
	}

	info := &LedgerInfo{
		Sequence:  ledger.Sequence,
		Hash:      ledger.Hash,
		CloseTime: closeTime,
	}

	// Cache the result
	c.cache.set(cacheKey, info, c.cache.maxAge)

	return info, nil
}

// EstimateLedgerAtTime estimates the ledger sequence at a given time
func (c *StellarNetworkClient) EstimateLedgerAtTime(ctx context.Context, targetTime time.Time) (uint32, error) {
	// Get current ledger as reference
	currentLedger, err := c.GetCurrentLedger(ctx)
	if err != nil {
		// Fall back to genesis calculation if network is unavailable
		return c.estimateFromGenesis(targetTime), nil
	}

	// If target time is in the future, return current ledger
	if targetTime.After(currentLedger.CloseTime) {
		return currentLedger.Sequence, nil
	}

	// Calculate time difference
	timeDiff := currentLedger.CloseTime.Sub(targetTime)
	log.Printf("DEBUG: Time diff: %v, current ledger: %d, current time: %v, target time: %v", 
		timeDiff, currentLedger.Sequence, currentLedger.CloseTime, targetTime)
	
	// Estimate ledgers based on average ledger time
	constants := networkConstants[c.network]
	ledgersDiff := timeDiff.Milliseconds() / constants.AverageLedgerTimeMs
	log.Printf("DEBUG: Network constants for %s: AverageLedgerTimeMs=%d", c.network, constants.AverageLedgerTimeMs)
	log.Printf("DEBUG: Ledgers diff calculation: %d ms / %d ms = %d ledgers", 
		timeDiff.Milliseconds(), constants.AverageLedgerTimeMs, ledgersDiff)
	
	estimatedSequence := int64(currentLedger.Sequence) - ledgersDiff
	log.Printf("DEBUG: Estimated sequence: %d - %d = %d", currentLedger.Sequence, ledgersDiff, estimatedSequence)
	if estimatedSequence < 1 {
		estimatedSequence = 1
	}

	// Use binary search to refine the estimate
	return c.binarySearchLedger(ctx, targetTime, uint32(estimatedSequence))
}

// ConvertTimeToLedger implements the LedgerTimeConverter interface
func (c *StellarNetworkClient) ConvertTimeToLedger(targetTime time.Time, network string) (uint32, error) {
	ctx := context.Background()
	return c.EstimateLedgerAtTime(ctx, targetTime)
}

// ConvertLedgerToTime implements the LedgerTimeConverter interface
func (c *StellarNetworkClient) ConvertLedgerToTime(ledgerSeq uint32, network string) (time.Time, error) {
	ctx := context.Background()
	ledgerInfo, err := c.GetLedgerBySequence(ctx, ledgerSeq)
	if err != nil {
		// Fall back to estimation if ledger not found
		return c.estimateTimeFromGenesis(ledgerSeq), nil
	}
	return ledgerInfo.CloseTime, nil
}

// GetCurrentLedgerForNetwork implements the LedgerTimeConverter interface
func (c *StellarNetworkClient) GetCurrentLedgerForNetwork(network string) (uint32, time.Time, error) {
	ctx := context.Background()
	ledgerInfo, err := c.GetCurrentLedger(ctx)
	if err != nil {
		return 0, time.Time{}, err
	}
	return ledgerInfo.Sequence, ledgerInfo.CloseTime, nil
}

// binarySearchLedger performs a binary search to find the ledger closest to the target time
func (c *StellarNetworkClient) binarySearchLedger(ctx context.Context, targetTime time.Time, initialEstimate uint32) (uint32, error) {
	// Set search bounds
	searchRadius := uint32(1000) // Search within 1000 ledgers of estimate
	minSeq := initialEstimate
	if minSeq > searchRadius {
		minSeq = initialEstimate - searchRadius
	} else {
		minSeq = MinAvailableLedger
	}
	maxSeq := initialEstimate + searchRadius

	// Limit iterations to prevent infinite loops
	maxIterations := 20
	iteration := 0

	for minSeq < maxSeq && iteration < maxIterations {
		iteration++
		midSeq := (minSeq + maxSeq) / 2

		ledgerInfo, err := c.GetLedgerBySequence(ctx, midSeq)
		if err != nil {
			// If ledger not found, adjust bounds
			if midSeq > initialEstimate {
				maxSeq = midSeq - 1
			} else {
				minSeq = midSeq + 1
			}
			continue
		}

		if ledgerInfo.CloseTime.Before(targetTime) {
			minSeq = midSeq + 1
		} else if ledgerInfo.CloseTime.After(targetTime) {
			maxSeq = midSeq - 1
		} else {
			// Exact match
			return midSeq, nil
		}
	}

	// Return the closest ledger
	return minSeq, nil
}

// estimateFromGenesis estimates ledger sequence based on genesis time and average ledger time
func (c *StellarNetworkClient) estimateFromGenesis(targetTime time.Time) uint32 {
	constants := networkConstants[c.network]
	
	// For testnet, use current ledger approach since testnet gets reset frequently
	// and genesis time is unreliable
	if c.network == "testnet" {
		return c.estimateFromCurrentLedger(targetTime)
	}
	
	if targetTime.Before(constants.GenesisTime) {
		return MinAvailableLedger
	}

	timeSinceGenesis := targetTime.Sub(constants.GenesisTime)
	estimatedLedgers := timeSinceGenesis.Milliseconds() / constants.AverageLedgerTimeMs
	
	if estimatedLedgers < int64(MinAvailableLedger) {
		return MinAvailableLedger
	}

	return uint32(estimatedLedgers)
}

// estimateFromCurrentLedger estimates ledger sequence by working backwards from current ledger
func (c *StellarNetworkClient) estimateFromCurrentLedger(targetTime time.Time) uint32 {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Get current ledger as reference
	currentLedger, err := c.GetCurrentLedger(ctx)
	if err != nil {
		log.Printf("Warning: Failed to get current ledger for estimation, using fallback: %v", err)
		// Fallback to a reasonable estimate based on current time
		constants := networkConstants[c.network]
		timeSinceTarget := time.Since(targetTime)
		ledgersDiff := timeSinceTarget.Milliseconds() / constants.AverageLedgerTimeMs
		return uint32(ledgersDiff) // Very rough estimate
	}
	
	// If target is in the future, return current
	if targetTime.After(currentLedger.CloseTime) {
		return currentLedger.Sequence
	}
	
	// Calculate backwards from current ledger
	timeDiff := currentLedger.CloseTime.Sub(targetTime)
	constants := networkConstants[c.network]
	ledgersDiff := timeDiff.Milliseconds() / constants.AverageLedgerTimeMs
	
	estimatedSequence := int64(currentLedger.Sequence) - ledgersDiff
	if estimatedSequence < int64(MinAvailableLedger) {
		return MinAvailableLedger
	}
	
	log.Printf("DEBUG: Estimate from current ledger - current: %d (%v), target: %v, diff: %d ledgers, result: %d", 
		currentLedger.Sequence, currentLedger.CloseTime, targetTime, ledgersDiff, estimatedSequence)
	
	return uint32(estimatedSequence)
}

// estimateTimeFromGenesis estimates time based on ledger sequence and genesis
func (c *StellarNetworkClient) estimateTimeFromGenesis(sequence uint32) time.Time {
	constants := networkConstants[c.network]
	
	ledgersSinceGenesis := int64(sequence - 1)
	millisecondsSinceGenesis := ledgersSinceGenesis * constants.AverageLedgerTimeMs
	
	return constants.GenesisTime.Add(time.Duration(millisecondsSinceGenesis) * time.Millisecond)
}

// GetLedgerRange queries a range of ledgers (for batch operations)
func (c *StellarNetworkClient) GetLedgerRange(ctx context.Context, startSeq, endSeq uint32) ([]LedgerInfo, error) {
	if endSeq < startSeq {
		return nil, fmt.Errorf("invalid range: end sequence must be >= start sequence")
	}

	// Limit range to prevent excessive API calls
	maxRange := uint32(200)
	if endSeq-startSeq > maxRange {
		endSeq = startSeq + maxRange
	}

	url := fmt.Sprintf("%s/ledgers?cursor=%d&order=asc&limit=%d", 
		c.horizonURL, startSeq-1, endSeq-startSeq+1)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query Horizon: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Horizon returned status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		Embedded struct {
			Records []HorizonLedgerResponse `json:"records"`
		} `json:"_embedded"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	ledgers := make([]LedgerInfo, 0, len(response.Embedded.Records))
	for _, record := range response.Embedded.Records {
		closeTime, err := time.Parse(time.RFC3339, record.ClosedAt)
		if err != nil {
			log.Printf("Warning: failed to parse close time for ledger %d: %v", record.Sequence, err)
			continue
		}

		ledgers = append(ledgers, LedgerInfo{
			Sequence:  record.Sequence,
			Hash:      record.Hash,
			CloseTime: closeTime,
		})
	}

	// Sort by sequence to ensure order
	sort.Slice(ledgers, func(i, j int) bool {
		return ledgers[i].Sequence < ledgers[j].Sequence
	})

	return ledgers, nil
}

// Cache methods
func (c *ledgerCache) get(key string) *LedgerInfo {
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

	return entry.ledgerInfo
}

func (c *ledgerCache) set(key string, info *LedgerInfo, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Use provided TTL or default maxAge
	if ttl == 0 {
		ttl = c.maxAge
	}

	c.entries[key] = &cacheEntry{
		ledgerInfo: info,
		cachedAt:   time.Now(),
	}

	// Simple cleanup of old entries (could be improved with LRU)
	if len(c.entries) > 1000 {
		c.cleanup()
	}
}

func (c *ledgerCache) cleanup() {
	// Remove expired entries
	now := time.Now()
	for key, entry := range c.entries {
		if now.Sub(entry.cachedAt) > c.maxAge {
			delete(c.entries, key)
		}
	}
}