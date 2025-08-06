package cmd

import (
	"fmt"
	"runtime"
	
	"github.com/spf13/cobra"
	"github.com/fatih/color"
)

// Version information injected via main package
var (
	Version   string
	GitCommit string
	BuildDate string
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Long:  "Display detailed version information about flowctl",
	Run: func(cmd *cobra.Command, args []string) {
		// Use color for better terminal output
		title := color.New(color.FgCyan, color.Bold)
		label := color.New(color.FgGreen)
		
		title.Printf("FlowCTL %s\n", getVersion())
		fmt.Println()
		
		label.Print("Git commit: ")
		fmt.Println(getGitCommit())
		
		label.Print("Built:      ")
		fmt.Println(getBuildDate())
		
		label.Print("Go version: ")
		fmt.Println(runtime.Version())
		
		label.Print("OS/Arch:    ")
		fmt.Printf("%s/%s\n", runtime.GOOS, runtime.GOARCH)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}

// Helper functions with fallbacks
func getVersion() string {
	if Version == "" {
		return "dev"
	}
	return Version
}

func getGitCommit() string {
	if GitCommit == "" {
		return "unknown"
	}
	return GitCommit
}

func getBuildDate() string {
	if BuildDate == "" {
		return "unknown"
	}
	return BuildDate
}

// SetVersionInfo sets the version information from the main package
func SetVersionInfo(version, gitCommit, buildDate string) {
	Version = version
	GitCommit = gitCommit
	BuildDate = buildDate
}