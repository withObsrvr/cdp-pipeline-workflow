package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	
	"github.com/spf13/cobra"
	"github.com/fatih/color"
	"github.com/withObsrvr/cdp-pipeline-workflow/internal/cli/runner"
)

var (
	// factories is set by main.go during initialization
	factories runner.Factories
	
	runCmd = &cobra.Command{
		Use:   "run [config file]",
		Short: "Run a pipeline from configuration",
		Long:  "Execute a CDP pipeline using the specified configuration file",
		Args:  cobra.ExactArgs(1),
		Example: `  flowctl run pipeline.yaml
  flowctl run config/production.yaml
  flowctl run stellar-pipeline.yaml`,
		RunE: runPipeline,
	}
)

func init() {
	rootCmd.AddCommand(runCmd)
}

// SetFactories sets the factory functions for creating pipeline components
func SetFactories(f runner.Factories) {
	factories = f
}

func runPipeline(cmd *cobra.Command, args []string) error {
	configFile := args[0]
	
	// Check if file exists
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		return fmt.Errorf("configuration file not found: %s", configFile)
	}
	
	// Pretty print startup
	fmt.Println(color.GreenString("🚀 Starting pipeline from %s", configFile))
	
	// Create context with signal handling
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	
	// Create and run pipeline
	runner := runner.New(runner.Options{
		ConfigFile: configFile,
		Verbose:    verbose,
	}, factories)
	
	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf("pipeline failed: %w", err)
	}
	
	fmt.Println(color.GreenString("✅ Pipeline completed successfully"))
	return nil
}