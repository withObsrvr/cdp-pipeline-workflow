package testing

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/withObsrvr/cdp-pipeline-workflow/internal/config/v2"
)

// ConfigEquivalenceChecker tests if two configurations produce the same result
type ConfigEquivalenceChecker struct {
	loader *v2.ConfigLoader
}

// NewConfigEquivalenceChecker creates a new equivalence checker
func NewConfigEquivalenceChecker() (*ConfigEquivalenceChecker, error) {
	loader, err := v2.NewConfigLoader(v2.DefaultLoaderOptions())
	if err != nil {
		return nil, err
	}

	return &ConfigEquivalenceChecker{
		loader: loader,
	}, nil
}

// EquivalenceResult contains the result of an equivalence check
type EquivalenceResult struct {
	Equivalent   bool
	Differences  []Difference
	LegacyResult *v2.LoadResult
	V2Result     *v2.LoadResult
}

// Difference represents a difference between configurations
type Difference struct {
	Path       string
	Legacy     interface{}
	V2         interface{}
	IsCritical bool
}

// CheckEquivalence checks if legacy and v2 configs are equivalent
func (c *ConfigEquivalenceChecker) CheckEquivalence(legacyConfig, v2Config map[string]interface{}) (*EquivalenceResult, error) {
	// Load both configurations
	legacyResult, err := c.loader.LoadFromData(legacyConfig)
	if err != nil {
		return nil, fmt.Errorf("loading legacy config: %w", err)
	}

	v2Result, err := c.loader.LoadFromData(v2Config)
	if err != nil {
		return nil, fmt.Errorf("loading v2 config: %w", err)
	}

	result := &EquivalenceResult{
		Equivalent:   true,
		LegacyResult: legacyResult,
		V2Result:     v2Result,
	}

	// Compare the transformed configurations
	differences := c.compareConfigs(legacyResult.Config, v2Result.Config, "")
	result.Differences = differences

	// Check if any critical differences
	for _, diff := range differences {
		if diff.IsCritical {
			result.Equivalent = false
			break
		}
	}

	return result, nil
}

// compareConfigs recursively compares two configurations
func (c *ConfigEquivalenceChecker) compareConfigs(legacy, v2 *v2.TransformedConfig, path string) []Difference {
	var differences []Difference

	// Compare number of pipelines
	if len(legacy.Pipelines) != len(v2.Pipelines) {
		// In v2, we often have a single "default" pipeline
		if len(v2.Pipelines) == 1 && len(legacy.Pipelines) == 1 {
			// This is okay - compare the single pipelines
			for legacyName, legacyPipeline := range legacy.Pipelines {
				for v2Name, v2Pipeline := range v2.Pipelines {
					// Names might differ, but content should match
					pipelineDiffs := c.comparePipelines(legacyPipeline, v2Pipeline, fmt.Sprintf("%s.pipelines", path))
					differences = append(differences, pipelineDiffs...)

					// Add non-critical difference for pipeline name
					if legacyName != v2Name {
						differences = append(differences, Difference{
							Path:       fmt.Sprintf("%s.pipeline_name", path),
							Legacy:     legacyName,
							V2:         v2Name,
							IsCritical: false, // Pipeline name difference is not critical
						})
					}
				}
			}
		} else {
			differences = append(differences, Difference{
				Path:       fmt.Sprintf("%s.pipelines.count", path),
				Legacy:     len(legacy.Pipelines),
				V2:         len(v2.Pipelines),
				IsCritical: true,
			})
		}
	} else {
		// Compare pipelines by name
		for name, legacyPipeline := range legacy.Pipelines {
			// Try exact name match first
			if v2Pipeline, ok := v2.Pipelines[name]; ok {
				pipelineDiffs := c.comparePipelines(legacyPipeline, v2Pipeline, fmt.Sprintf("%s.pipelines.%s", path, name))
				differences = append(differences, pipelineDiffs...)
			} else if len(legacy.Pipelines) == 1 && len(v2.Pipelines) == 1 {
				// If both have single pipelines with different names, compare them anyway
				// This handles the case where v2 creates a "default" pipeline
				for v2Name, v2Pipeline := range v2.Pipelines {
					pipelineDiffs := c.comparePipelines(legacyPipeline, v2Pipeline, fmt.Sprintf("%s.pipelines", path))
					differences = append(differences, pipelineDiffs...)
					// Add non-critical difference for name mismatch
					differences = append(differences, Difference{
						Path:       fmt.Sprintf("%s.pipeline_name", path),
						Legacy:     name,
						V2:         v2Name,
						IsCritical: false,
					})
				}
			} else {
				differences = append(differences, Difference{
					Path:       fmt.Sprintf("%s.pipelines.%s", path, name),
					Legacy:     "exists",
					V2:         "missing",
					IsCritical: true,
				})
			}
		}
	}

	return differences
}

// comparePipelines compares two pipeline configurations
func (c *ConfigEquivalenceChecker) comparePipelines(legacy, v2 v2.TransformedPipeline, path string) []Difference {
	var differences []Difference

	// Compare source
	sourceDiffs := c.compareComponents(legacy.Source, v2.Source, fmt.Sprintf("%s.source", path))
	differences = append(differences, sourceDiffs...)

	// Compare processors
	if len(legacy.Processors) != len(v2.Processors) {
		differences = append(differences, Difference{
			Path:       fmt.Sprintf("%s.processors.count", path),
			Legacy:     len(legacy.Processors),
			V2:         len(v2.Processors),
			IsCritical: true,
		})
	} else {
		for i := range legacy.Processors {
			procDiffs := c.compareComponents(legacy.Processors[i], v2.Processors[i], fmt.Sprintf("%s.processors[%d]", path, i))
			differences = append(differences, procDiffs...)
		}
	}

	// Compare consumers
	if len(legacy.Consumers) != len(v2.Consumers) {
		differences = append(differences, Difference{
			Path:       fmt.Sprintf("%s.consumers.count", path),
			Legacy:     len(legacy.Consumers),
			V2:         len(v2.Consumers),
			IsCritical: true,
		})
	} else {
		for i := range legacy.Consumers {
			consDiffs := c.compareComponents(legacy.Consumers[i], v2.Consumers[i], fmt.Sprintf("%s.consumers[%d]", path, i))
			differences = append(differences, consDiffs...)
		}
	}

	return differences
}

// compareComponents compares two component configurations
func (c *ConfigEquivalenceChecker) compareComponents(legacy, v2 v2.TransformedComponent, path string) []Difference {
	var differences []Difference

	// Compare types
	if legacy.Type != v2.Type {
		differences = append(differences, Difference{
			Path:       fmt.Sprintf("%s.type", path),
			Legacy:     legacy.Type,
			V2:         v2.Type,
			IsCritical: true,
		})
	}

	// Compare configs
	configDiffs := c.compareConfigMaps(legacy.Config, v2.Config, fmt.Sprintf("%s.config", path))
	differences = append(differences, configDiffs...)

	return differences
}

// compareConfigMaps compares two configuration maps
func (c *ConfigEquivalenceChecker) compareConfigMaps(legacy, v2 map[string]interface{}, path string) []Difference {
	var differences []Difference
	matchedV2Keys := make(map[string]bool)

	// Check all keys in legacy
	for key, legacyValue := range legacy {
		if v2Value, ok := v2[key]; ok {
			matchedV2Keys[key] = true
			if !c.valuesEqualForKey(key, legacyValue, v2Value) {
				// Check if this is a default value difference
				isCritical := !c.isDefaultValueDifference(key, legacyValue, v2Value)
				differences = append(differences, Difference{
					Path:       fmt.Sprintf("%s.%s", path, key),
					Legacy:     legacyValue,
					V2:         v2Value,
					IsCritical: isCritical,
				})
			}
		} else if aliasKey, ok := c.configAlias(key); ok {
			if aliasValue, ok := v2[aliasKey]; ok {
				matchedV2Keys[aliasKey] = true
				if !c.valuesEqualForKey(key, legacyValue, aliasValue) {
					differences = append(differences, Difference{
						Path:       fmt.Sprintf("%s.%s", path, key),
						Legacy:     legacyValue,
						V2:         aliasValue,
						IsCritical: !c.isDefaultValueDifference(key, legacyValue, aliasValue),
					})
				}
			} else if !c.isDefaultValue(key, legacyValue) {
				differences = append(differences, Difference{
					Path:       fmt.Sprintf("%s.%s", path, key),
					Legacy:     legacyValue,
					V2:         "missing",
					IsCritical: true,
				})
			}
		} else {
			// Key missing in v2 - might be using a default
			if !c.isDefaultValue(key, legacyValue) {
				differences = append(differences, Difference{
					Path:       fmt.Sprintf("%s.%s", path, key),
					Legacy:     legacyValue,
					V2:         "missing",
					IsCritical: true,
				})
			}
		}
	}

	// Check for keys in v2 that aren't in legacy or already matched through an alias.
	for key, v2Value := range v2 {
		if matchedV2Keys[key] {
			continue
		}
		if _, ok := legacy[key]; !ok {
			// This might be a new default in v2
			if !c.isDefaultValue(key, v2Value) {
				differences = append(differences, Difference{
					Path:       fmt.Sprintf("%s.%s", path, key),
					Legacy:     "missing",
					V2:         v2Value,
					IsCritical: false, // New defaults are not critical
				})
			}
		}
	}

	return differences
}

// valuesEqualForKey checks if two values for the given config key are equal.
//
// Numeric strings are coerced only for keys known to hold numbers. YAML readily
// turns 100 into "100", so that coercion is needed — but applying it to every
// string makes a string-typed field whose value happens to be digits compare
// equal to a number (legacy bucket_name: "123" vs v2 bucket: 123), which the
// loaders would reject at runtime. Blessing that as equivalent would hide a
// broken migration, so the coercion is restricted by key.
func (c *ConfigEquivalenceChecker) valuesEqualForKey(key string, a, b interface{}) bool {
	if c.numericEqual(a, b) {
		return true
	}

	if isNumericConfigKey(key) {
		aFloat, aOk := toFloat64WithNumericStrings(a)
		bFloat, bOk := toFloat64WithNumericStrings(b)
		if aOk && bOk && aFloat == bFloat {
			return true
		}
	}

	return reflect.DeepEqual(a, b)
}

// numericEqual handles comparison of numeric values that might have different types
func (c *ConfigEquivalenceChecker) numericEqual(a, b interface{}) bool {
	// Convert to float64 for comparison
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)

	if aOk && bOk {
		return aFloat == bFloat
	}

	return false
}

// isNumericConfigKey reports whether a config key holds a number, and so may
// have been written as a quoted string in YAML. Add new numeric keys here; a
// key that is missing only causes a spurious difference to be reported, never a
// spurious equivalence.
func isNumericConfigKey(key string) bool {
	numericKeys := map[string]bool{
		"start_ledger": true, "end_ledger": true,
		"num_workers": true, "workers": true,
		"buffer_size": true, "buffer": true,
		"batch_size": true, "batch": true,
		"retry_limit": true, "retry_wait": true,
		"ledgers_per_file": true, "files_per_partition": true,
		"max_file_size_mb": true, "rotation_interval_minutes": true,
		"connection_pool_size": true, "max_open_conns": true, "max_idle_conns": true,
		"high_water_mark": true, "send_timeout_ms": true,
		"min_amount": true, "min": true,
		"max_amount": true, "max": true,
		"port": true, "redis_db": true, "limit": true,
		"poll_interval": true, "flush_interval": true, "flush_interval_seconds": true,
		"timeout": true, "connect_timeout": true,
		"time_to_live": true, "ttl": true,
	}
	return numericKeys[key]
}

// toFloat64 converts various numeric types to float64. Strings are deliberately
// not accepted; see toFloat64WithNumericStrings.
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	default:
		return 0, false
	}
}

// toFloat64WithNumericStrings is toFloat64 plus parsing of numeric strings. Use
// only for keys that isNumericConfigKey accepts.
func toFloat64WithNumericStrings(v interface{}) (float64, bool) {
	if f, ok := toFloat64(v); ok {
		return f, true
	}
	if s, ok := v.(string); ok {
		parsed, err := strconv.ParseFloat(s, 64)
		return parsed, err == nil
	}
	return 0, false
}

// isDefaultValueDifference checks if a difference is due to default values
func (c *ConfigEquivalenceChecker) isDefaultValueDifference(key string, legacy, v2 interface{}) bool {
	// Common default value differences that are not critical
	defaultDifferences := map[string]bool{
		"num_workers":    true,
		"buffer_size":    true,
		"retry_limit":    true,
		"retry_wait":     true,
		"batch_size":     true,
		"compression":    true,
		"create_table":   true,
		"include_failed": true,
	}

	if defaultDifferences[key] {
		return true
	}

	// history_archive_urls is only non-critical when both sides are stock SDF
	// archives — that is the case where v2 filled in network defaults. A custom
	// or private archive on either side changes where ingestion reads from, so
	// it must stay critical: otherwise a migration that silently repointed at
	// public archives (or at a mirror unreachable in restricted environments)
	// would be reported as equivalent.
	if key == "history_archive_urls" {
		return isDefaultHistoryArchiveSet(legacy) && isDefaultHistoryArchiveSet(v2)
	}

	return false
}

// defaultHistoryArchiveURLs is the set of stock SDF archives that
// internal/config/v2 injects as network defaults.
var defaultHistoryArchiveURLs = map[string]bool{
	"https://history.stellar.org/prd/core-live-001":    true,
	"https://history.stellar.org/prd/core-live-002":    true,
	"https://history.stellar.org/prd/core-live-003":    true,
	"https://history.stellar.org/prd/core-testnet-001": true,
	"https://history.stellar.org/prd/core-testnet-002": true,
	"https://history.stellar.org/prd/core-testnet-003": true,
	"https://history-futurenet.stellar.org":            true,
}

// isDefaultHistoryArchiveSet reports whether every URL in the value is a stock
// SDF archive. An empty or unrecognised value is not a default set.
func isDefaultHistoryArchiveSet(value interface{}) bool {
	urls, ok := toStringSlice(value)
	if !ok || len(urls) == 0 {
		return false
	}
	for _, u := range urls {
		if !defaultHistoryArchiveURLs[strings.TrimRight(u, "/")] {
			return false
		}
	}
	return true
}

// toStringSlice normalises the shapes a YAML list of strings can arrive in.
func toStringSlice(value interface{}) ([]string, bool) {
	switch v := value.(type) {
	case []string:
		return v, true
	case string:
		return []string{v}, true
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, false
			}
			out = append(out, s)
		}
		return out, true
	default:
		return nil, false
	}
}

// configAlias maps equivalent config keys between loaded legacy and v2 configs.
func (c *ConfigEquivalenceChecker) configAlias(key string) (string, bool) {
	aliases := map[string]string{
		"bucket_name":       "bucket",
		"bucket":            "bucket_name",
		"path_prefix":       "path",
		"path":              "path_prefix",
		"table_name":        "table",
		"table":             "table_name",
		"num_workers":       "workers",
		"workers":           "num_workers",
		"buffer_size":       "buffer",
		"buffer":            "buffer_size",
		"batch_size":        "batch",
		"batch":             "batch_size",
		"connection_string": "connection",
		"connection":        "connection_string",
		// Keep the pairs below in sync with getFieldAlias in
		// internal/cli/cmd/config.go; a pair missing here surfaces as a spurious
		// critical difference during equivalence checks.
		"min_amount":         "min",
		"min":                "min_amount",
		"max_amount":         "max",
		"max":                "max_amount",
		"time_to_live":       "ttl",
		"ttl":                "time_to_live",
		"network_passphrase": "passphrase",
		"passphrase":         "network_passphrase",
	}
	alias, ok := aliases[key]
	return alias, ok
}

// isDefaultValue checks if a value is a known default
func (c *ConfigEquivalenceChecker) isDefaultValue(key string, value interface{}) bool {
	knownDefaults := map[string]interface{}{
		"num_workers":      20,
		"buffer_size":      10000,
		"retry_limit":      3,
		"retry_wait":       5,
		"batch_size":       1000,
		"compression":      "snappy",
		"create_table":     true,
		"include_failed":   false,
		"schema_evolution": true,
	}

	if defaultVal, ok := knownDefaults[key]; ok {
		return c.valuesEqualForKey(key, value, defaultVal)
	}

	return false
}

// GenerateReport generates a human-readable report of differences
func (r *EquivalenceResult) GenerateReport() string {
	if r.Equivalent {
		return "✅ Configurations are functionally equivalent\n"
	}

	report := "❌ Configurations have critical differences:\n\n"

	criticalCount := 0
	nonCriticalCount := 0

	for _, diff := range r.Differences {
		if diff.IsCritical {
			criticalCount++
			report += fmt.Sprintf("🔴 %s:\n   Legacy: %v\n   V2: %v\n\n", diff.Path, diff.Legacy, diff.V2)
		} else {
			nonCriticalCount++
		}
	}

	if nonCriticalCount > 0 {
		report += fmt.Sprintf("\nℹ️  Also found %d non-critical differences (defaults, names, etc.)\n", nonCriticalCount)
	}

	return report
}
