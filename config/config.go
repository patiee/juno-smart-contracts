package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	DbUser        string   `json:"db_user"`
	DbPassword    string   `json:"db_password"`
	DbName        string   `json:"db_name"`
	LogLevel      string   `json:"log_level"`
	GrpcUrl       string   `json:"grpc_url"`
	ResolversPath string   `json:"resolvers_path"`
	SchemaPath    string   `json:"schema_path"`
	Messages      []string `json:"messages"`
}

func ReadConfig(path string) (*Config, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err = json.Unmarshal(file, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
