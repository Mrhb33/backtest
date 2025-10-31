package engine

// Config and secrets discipline

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
)

type ConfigSnapshot struct {
	Environment string            `json:"environment"`
	Version     string            `json:"version"`
	ConfigHash  string            `json:"config_hash"`
	SecretsHash string            `json:"secrets_hash"`
	Timestamp   uint64            `json:"timestamp"`
	Values      map[string]string `json:"values"`
}

type ConfigManager struct {
	configs map[string]*ConfigSnapshot
}

func NewConfigManager() *ConfigManager {
	return &ConfigManager{
		configs: make(map[string]*ConfigSnapshot),
	}
}

func (cm *ConfigManager) SnapshotConfig(env, version string, config, secrets map[string]string) *ConfigSnapshot {
	configBytes, _ := json.Marshal(config)
	secretsBytes, _ := json.Marshal(secrets)

	configHash := fmt.Sprintf("%x", sha256.Sum256(configBytes))
	secretsHash := fmt.Sprintf("%x", sha256.Sum256(secretsBytes))

	snapshot := &ConfigSnapshot{
		Environment: env,
		Version:     version,
		ConfigHash:  configHash,
		SecretsHash: secretsHash,
		Timestamp:   uint64(time.Now().UnixMilli()),
		Values:      make(map[string]string),
	}

	// Copy config values (not secrets)
	for k, v := range config {
		snapshot.Values[k] = v
	}

	cm.configs[version] = snapshot
	return snapshot
}

func (cm *ConfigManager) GetSnapshot(version string) (*ConfigSnapshot, bool) {
	snapshot, exists := cm.configs[version]
	return snapshot, exists
}

// Run manifest with full reproducibility
type ReproducibleManifest struct {
	JobID          string          `json:"job_id"`
	ConfigSnapshot *ConfigSnapshot `json:"config_snapshot"`
	DataChecksum   string          `json:"data_checksum"`
	StrategyHash   string          `json:"strategy_hash"`
	EngineVersion  string          `json:"engine_version"`
	CreatedAt      uint64          `json:"created_at"`
}
