package cmd

import (
	"fmt"
	"os"
	
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/fatih/color"
)

var (
	cfgFile string
	verbose bool
	
	rootCmd = &cobra.Command{
		Use:   "flowctl",
		Short: "CDP Pipeline Workflow CLI",
		Long: color.CyanString(`CDP Pipeline Workflow - Run data pipelines with ease`),
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)
	
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: $HOME/.flowctl.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
	
	// Bind flags to viper
	viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose"))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, _ := os.UserHomeDir()
		viper.AddConfigPath(home)
		viper.SetConfigName(".flowctl")
	}
	
	viper.AutomaticEnv()
	viper.SetEnvPrefix("FLOWCTL")
	
	if err := viper.ReadInConfig(); err == nil && verbose {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}