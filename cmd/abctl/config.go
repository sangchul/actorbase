package main

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Config는 abctl 전역 설정을 담는다.
type Config struct {
	PMAddr string `json:"pm_addr"` // PM gRPC 주소
}

// loadConfig는 설정을 우선순위 순서로 로드한다: 플래그 > 환경변수 > 설정 파일 > 기본값.
// flagPMAddr이 빈 문자열이면 플래그가 제공되지 않은 것으로 간주한다.
func loadConfig(flagPMAddr string) *Config {
	cfg := &Config{
		PMAddr: "localhost:7000",
	}

	// 설정 파일: ~/.actorbase/config.json
	if home, err := os.UserHomeDir(); err == nil {
		path := filepath.Join(home, ".actorbase", "config.json")
		if data, err := os.ReadFile(path); err == nil {
			_ = json.Unmarshal(data, cfg)
		}
	}

	// 환경변수
	if v := os.Getenv("ACTORBASE_PM_ADDR"); v != "" {
		cfg.PMAddr = v
	}

	// 플래그 (최우선)
	if flagPMAddr != "" {
		cfg.PMAddr = flagPMAddr
	}

	return cfg
}
