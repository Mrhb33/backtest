package config

type ServerConfig struct {
	HTTPPort int
	GRPCPort int
}

type EngineConfig struct {
	MaxWorkers int
}

type ClickHouseConfig struct{}

type ArrowConfig struct{}

type MonitoringConfig struct{}

type Config struct {
	Environment string
	Server      ServerConfig
	Engine      EngineConfig
	ClickHouse  ClickHouseConfig
	Arrow       ArrowConfig
	Monitoring  MonitoringConfig
}

func Load() (*Config, error) {
	return &Config{Environment: "dev", Server: ServerConfig{HTTPPort: 8080, GRPCPort: 9091}}, nil
}
