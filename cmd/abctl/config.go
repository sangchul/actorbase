package main

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Config holds the global abctl configuration.
type Config struct {
	PMAddr string `json:"pm_addr"` // PM gRPC address
}

// loadConfig loads configuration in priority order: flag > env var > config file > default.
// If flagPMAddr is an empty string, the flag is considered not provided.
func loadConfig(flagPMAddr string) *Config {
	cfg := &Config{
		PMAddr: "localhost:7000",
	}

	// Config file: ~/.actorbase/config.json
	if home, err := os.UserHomeDir(); err == nil {
		path := filepath.Join(home, ".actorbase", "config.json")
		if data, err := os.ReadFile(path); err == nil {
			_ = json.Unmarshal(data, cfg)
		}
	}

	// Environment variable
	if v := os.Getenv("ACTORBASE_PM_ADDR"); v != "" {
		cfg.PMAddr = v
	}

	// Flag (highest priority)
	if flagPMAddr != "" {
		cfg.PMAddr = flagPMAddr
	}

	return cfg
}
