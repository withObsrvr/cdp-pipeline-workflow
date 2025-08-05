package cmd

import (
	"fmt"
	"os"
	"strings"
	
	"github.com/fatih/color"
	"github.com/spf13/cobra"
	v2config "github.com/withObsrvr/cdp-pipeline-workflow/internal/config/v2"
	"gopkg.in/yaml.v3"
)

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Configuration management commands",
	Long:  `Commands for validating, upgrading, and managing pipeline configurations.`,
}

// validateCmd validates a configuration file
var validateCmd = &cobra.Command{
	Use:   "validate [config file]",
	Short: "Validate a configuration file",
	Long:  `Validate a pipeline configuration file and report any errors or warnings.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		configFile := args[0]
		
		// Check if file exists
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			return fmt.Errorf("config file does not exist: %s", configFile)
		}
		
		// Create config loader
		loader, err := v2config.NewConfigLoader(v2config.DefaultLoaderOptions())
		if err != nil {
			return fmt.Errorf("creating config loader: %w", err)
		}
		
		// Validate the config
		result, err := loader.ValidateFile(configFile)
		if err != nil {
			return fmt.Errorf("reading config file: %w", err)
		}
		
		// Display results
		if result.HasErrors() {
			color.Red("❌ Configuration has errors:\n")
			for _, err := range result.Errors {
				fmt.Printf("  • %v\n", err)
			}
			return fmt.Errorf("configuration validation failed")
		}
		
		if len(result.Warnings) > 0 {
			color.Yellow("⚠️  Configuration has warnings:\n")
			for _, warning := range result.Warnings {
				fmt.Printf("  • %s\n", warning)
			}
		}
		
		color.Green("✅ Configuration is valid!")
		return nil
	},
}

// explainCmd explains what a configuration does
var explainCmd = &cobra.Command{
	Use:   "explain [config file]",
	Short: "Explain what a configuration does",
	Long:  `Provide detailed information about a pipeline configuration, including components and their purposes.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		configFile := args[0]
		
		// Check if file exists
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			return fmt.Errorf("config file does not exist: %s", configFile)
		}
		
		// Create config loader
		loader, err := v2config.NewConfigLoader(v2config.DefaultLoaderOptions())
		if err != nil {
			return fmt.Errorf("creating config loader: %w", err)
		}
		
		// Load and explain the config
		explanation, err := loader.ExplainConfig(configFile)
		if err != nil {
			return fmt.Errorf("explaining config: %w", err)
		}
		
		// Display explanation
		formatName := "Unknown"
		switch explanation.Format {
		case v2config.FormatLegacy:
			formatName = "Legacy"
		case v2config.FormatV2:
			formatName = "Simplified (v2)"
		}
		
		fmt.Printf("📄 Configuration Format: %s\n\n", formatName)
		
		for name, pipeline := range explanation.Pipelines {
			color.Cyan("Pipeline: %s\n", name)
			fmt.Println(strings.Repeat("─", 40))
			
			// Source
			if pipeline.Source.Type != "" {
				fmt.Printf("\n📥 Source: %s\n", pipeline.Source.Type)
				fmt.Printf("   Description: %s\n", pipeline.Source.Description)
				if len(pipeline.Source.Aliases) > 0 {
					fmt.Printf("   Aliases: %s\n", strings.Join(pipeline.Source.Aliases, ", "))
				}
			}
			
			// Processors
			if len(pipeline.Processors) > 0 {
				fmt.Printf("\n⚙️  Processors (%d):\n", len(pipeline.Processors))
				for i, proc := range pipeline.Processors {
					fmt.Printf("   %d. %s - %s\n", i+1, proc.Type, proc.Description)
					if len(proc.Aliases) > 0 {
						fmt.Printf("      Aliases: %s\n", strings.Join(proc.Aliases, ", "))
					}
				}
			}
			
			// Consumers
			if len(pipeline.Consumers) > 0 {
				fmt.Printf("\n💾 Consumers (%d):\n", len(pipeline.Consumers))
				for i, cons := range pipeline.Consumers {
					fmt.Printf("   %d. %s - %s\n", i+1, cons.Type, cons.Description)
					if len(cons.Aliases) > 0 {
						fmt.Printf("      Aliases: %s\n", strings.Join(cons.Aliases, ", "))
					}
				}
			}
			
			fmt.Println()
		}
		
		return nil
	},
}

// upgradeCmd upgrades a legacy config to v2 format
var upgradeCmd = &cobra.Command{
	Use:   "upgrade [config file]",
	Short: "Upgrade a legacy configuration to simplified v2 format",
	Long:  `Convert a legacy configuration file to the new simplified v2 format, reducing verbosity while maintaining functionality.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		configFile := args[0]
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		output, _ := cmd.Flags().GetString("output")
		
		// Check if file exists
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			return fmt.Errorf("config file does not exist: %s", configFile)
		}
		
		// Create config loader
		loader, err := v2config.NewConfigLoader(v2config.DefaultLoaderOptions())
		if err != nil {
			return fmt.Errorf("creating config loader: %w", err)
		}
		
		// Load the config
		result, err := loader.Load(configFile)
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}
		
		// Check if already v2
		if result.Format == v2config.FormatV2 {
			color.Yellow("ℹ️  Configuration is already in v2 format")
			return nil
		}
		
		// Convert to v2 format
		v2Config := convertToV2Format(result.Config)
		
		// Marshal to YAML
		yamlData, err := yaml.Marshal(v2Config)
		if err != nil {
			return fmt.Errorf("marshaling v2 config: %w", err)
		}
		
		// Add header comment
		v2Yaml := fmt.Sprintf("# Simplified v2 configuration\n# Generated by: flowctl config upgrade\n# Original file: %s\n\n%s", configFile, string(yamlData))
		
		if dryRun {
			fmt.Println("🔍 Preview of upgraded configuration:")
			fmt.Println(strings.Repeat("─", 60))
			fmt.Print(v2Yaml)
			fmt.Println(strings.Repeat("─", 60))
			
			// Show size reduction
			originalSize := getFileSize(configFile)
			newSize := len(v2Yaml)
			reduction := float64(originalSize-newSize) / float64(originalSize) * 100
			
			color.Green("\n📊 Size reduction: %.1f%% (%d → %d bytes)", reduction, originalSize, newSize)
			fmt.Println("\nRun without --dry-run to save the upgraded configuration")
			return nil
		}
		
		// Determine output file
		outputFile := output
		if outputFile == "" {
			// Default to same name with .v2.yaml extension
			outputFile = strings.TrimSuffix(configFile, ".yaml") + ".v2.yaml"
		}
		
		// Create backup if overwriting
		if outputFile == configFile {
			backupFile := configFile + ".backup"
			if err := copyFile(configFile, backupFile); err != nil {
				return fmt.Errorf("creating backup: %w", err)
			}
			color.Green("✅ Backup saved to: %s", backupFile)
		}
		
		// Write upgraded config
		if err := os.WriteFile(outputFile, []byte(v2Yaml), 0644); err != nil {
			return fmt.Errorf("writing upgraded config: %w", err)
		}
		
		color.Green("✅ Configuration upgraded successfully!")
		fmt.Printf("   Output: %s\n", outputFile)
		
		// Show size reduction
		originalSize := getFileSize(configFile)
		newSize := len(v2Yaml)
		reduction := float64(originalSize-newSize) / float64(originalSize) * 100
		fmt.Printf("   Size reduction: %.1f%%\n", reduction)
		
		return nil
	},
}

// examplesCmd shows configuration examples
var examplesCmd = &cobra.Command{
	Use:   "examples",
	Short: "Show configuration examples",
	Long:  `Display examples of both legacy and v2 simplified configurations.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("📚 Configuration Examples\n")
		
		color.Cyan("Legacy Configuration:")
		fmt.Println(strings.Repeat("─", 60))
		fmt.Println(v2config.GetLegacyExample())
		
		fmt.Println()
		color.Cyan("Simplified v2 Configuration:")
		fmt.Println(strings.Repeat("─", 60))
		fmt.Println(v2config.GetSimplifiedExample())
		
		fmt.Println("\n💡 The v2 configuration achieves the same result with ~80% less code!")
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
	
	// Add subcommands
	configCmd.AddCommand(validateCmd)
	configCmd.AddCommand(explainCmd)
	configCmd.AddCommand(upgradeCmd)
	configCmd.AddCommand(examplesCmd)
	
	// Add flags
	upgradeCmd.Flags().BoolP("dry-run", "d", false, "Preview the upgraded configuration without saving")
	upgradeCmd.Flags().StringP("output", "o", "", "Output file (default: input.v2.yaml)")
}

// Helper functions

func getFileSize(path string) int {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return int(info.Size())
}

func copyFile(src, dst string) error {
	input, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, input, 0644)
}

// convertToV2Format converts a loaded configuration to v2 format
func convertToV2Format(config *v2config.TransformedConfig) map[string]interface{} {
	// For single pipeline configs, we can use the simplified format
	if len(config.Pipelines) == 1 {
		for _, pipeline := range config.Pipelines {
			return createV2Pipeline(pipeline)
		}
	}
	
	// For multi-pipeline configs, we need to keep the pipelines structure
	v2Config := make(map[string]interface{})
	pipelines := make(map[string]interface{})
	
	for name, pipeline := range config.Pipelines {
		pipelines[name] = createV2Pipeline(pipeline)
	}
	
	v2Config["pipelines"] = pipelines
	return v2Config
}

func createV2Pipeline(pipeline v2config.TransformedPipeline) map[string]interface{} {
	result := make(map[string]interface{})
	
	// Convert source
	if pipeline.Source.Type != "" {
		source := simplifyComponent(pipeline.Source.Type, pipeline.Source.Config)
		if source != nil {
			result["source"] = source
		}
	}
	
	// Convert processors
	if len(pipeline.Processors) > 0 {
		processors := []interface{}{}
		for _, proc := range pipeline.Processors {
			if simplified := simplifyComponent(proc.Type, proc.Config); simplified != nil {
				processors = append(processors, simplified)
			}
		}
		if len(processors) == 1 {
			result["process"] = processors[0]
		} else if len(processors) > 1 {
			result["process"] = processors
		}
	}
	
	// Convert consumers
	if len(pipeline.Consumers) > 0 {
		consumers := []interface{}{}
		for _, cons := range pipeline.Consumers {
			if simplified := simplifyComponent(cons.Type, cons.Config); simplified != nil {
				consumers = append(consumers, simplified)
			}
		}
		if len(consumers) == 1 {
			result["save_to"] = consumers[0]
		} else if len(consumers) > 1 {
			result["save_to"] = consumers
		}
	}
	
	return result
}

func simplifyComponent(componentType string, config map[string]interface{}) interface{} {
	// Get the best alias for this component
	alias := getPreferredAlias(componentType)
	
	// Remove default values from config
	cleanConfig := removeDefaults(componentType, config)
	
	// If no config remains, just return the alias
	if len(cleanConfig) == 0 {
		return alias
	}
	
	// Return as a map with the alias as key
	return map[string]interface{}{
		alias: cleanConfig,
	}
}

func getPreferredAlias(componentType string) string {
	// Mapping of types to their preferred aliases
	aliases := map[string]string{
		"BufferedStorageSourceAdapter": "bucket",
		"CaptiveCoreInboundAdapter":    "stellar",
		"FilterPayments":               "payment_filter",
		"ContractData":                 "contract_data",
		"SaveToParquet":                "parquet",
		"SaveToPostgreSQL":             "postgres",
		"SaveToMongoDB":                "mongo",
		"SaveToRedis":                  "redis",
		"SaveToZeroMQ":                 "zmq",
		"LatestLedger":                 "latest_ledger",
		// Add more as needed
	}
	
	if alias, ok := aliases[componentType]; ok {
		return alias
	}
	
	// Default: lowercase the type and remove common suffixes
	simplified := strings.ToLower(componentType)
	simplified = strings.TrimPrefix(simplified, "saveto")
	simplified = strings.TrimSuffix(simplified, "adapter")
	simplified = strings.TrimSuffix(simplified, "processor")
	
	return simplified
}

func removeDefaults(componentType string, config map[string]interface{}) map[string]interface{} {
	// Common defaults to remove
	defaults := map[string]interface{}{
		"num_workers":                20,
		"retry_limit":                3,
		"retry_wait":                 5,
		"batch_size":                 1000,
		"buffer_size":                10000,
		"compression":                "snappy",
		"include_failed":             false,
		"schema_evolution":           true,
		"create_table":               true,
		"connection_pool_size":       10,
		"ledgers_per_file":           1,
		"files_per_partition":        64000,
		"rotation_interval_minutes":  60,
		"max_file_size_mb":           128,
	}
	
	// Create clean config without defaults
	clean := make(map[string]interface{})
	for key, value := range config {
		if defaultValue, hasDefault := defaults[key]; hasDefault {
			// Skip if value equals default
			if fmt.Sprintf("%v", value) == fmt.Sprintf("%v", defaultValue) {
				continue
			}
		}
		// Also use field aliases for cleaner output
		cleanKey := getFieldAlias(key)
		clean[cleanKey] = value
	}
	
	return clean
}

func getFieldAlias(field string) string {
	aliases := map[string]string{
		"bucket_name":         "bucket",
		"path_prefix":         "path",
		"num_workers":         "workers",
		"buffer_size":         "buffer",
		"batch_size":          "batch",
		"min_amount":          "min",
		"max_amount":          "max",
		"connection_string":   "connection",
		"time_to_live":        "ttl",
		"network_passphrase":  "passphrase",
	}
	
	if alias, ok := aliases[field]; ok {
		return alias
	}
	
	return field
}