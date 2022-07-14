package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	DB_User       string `json:"db_user"`
	DB_Password   string `json:"db_password"`
	DB_Name       string `json:"db_name"`
	SchemaPath    string `json:"schema_path"`
	ResolversPath string `json:"resolvers_path"`
}

func ReadConfig(path string) (*Config, error) {
	fmt.Println("Reading config")

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
