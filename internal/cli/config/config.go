package config

import (
	"github.com/spf13/viper"
)

type FlowctlConfig struct {
	DefaultSource     string            `mapstructure:"default_source"`
	DefaultConsumer   string            `mapstructure:"default_consumer"`
	Environments      map[string]EnvConfig `mapstructure:"environments"`
}

type EnvConfig struct {
	Name      string            `mapstructure:"name"`
	Variables map[string]string `mapstructure:"variables"`
}

func Load() (*FlowctlConfig, error) {
	var cfg FlowctlConfig
	
	// Set defaults
	viper.SetDefault("default_source", "BufferedStorageSourceAdapter")
	viper.SetDefault("default_consumer", "SaveToParquet")
	
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	
	return &cfg, nil
}

// Helper functions
func SetDefault(key, value string) {
	viper.Set(key, value)
	viper.WriteConfig()
}

func GetString(key string) string {
	return viper.GetString(key)
}