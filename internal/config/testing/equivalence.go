package testing

import (
	"fmt"
	"reflect"
	
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
	Path     string
	Legacy   interface{}
	V2       interface{}
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
	
	// Check all keys in legacy
	for key, legacyValue := range legacy {
		if v2Value, ok := v2[key]; ok {
			if !c.valuesEqual(legacyValue, v2Value) {
				// Check if this is a default value difference
				isCritical := !c.isDefaultValueDifference(key, legacyValue, v2Value)
				differences = append(differences, Difference{
					Path:       fmt.Sprintf("%s.%s", path, key),
					Legacy:     legacyValue,
					V2:         v2Value,
					IsCritical: isCritical,
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
	
	// Check for keys in v2 that aren't in legacy
	for key, v2Value := range v2 {
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

// valuesEqual checks if two values are equal
func (c *ConfigEquivalenceChecker) valuesEqual(a, b interface{}) bool {
	// Handle numeric comparisons specially
	if c.numericEqual(a, b) {
		return true
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

// toFloat64 converts various numeric types to float64
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
	default:
		return 0, false
	}
}

// isDefaultValueDifference checks if a difference is due to default values
func (c *ConfigEquivalenceChecker) isDefaultValueDifference(key string, legacy, v2 interface{}) bool {
	// Common default value differences that are not critical
	defaultDifferences := map[string]bool{
		"num_workers":     true,
		"buffer_size":     true,
		"retry_limit":     true,
		"retry_wait":      true,
		"batch_size":      true,
		"compression":     true,
		"create_table":    true,
		"include_failed":  true,
	}
	
	return defaultDifferences[key]
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
		return c.valuesEqual(value, defaultVal)
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