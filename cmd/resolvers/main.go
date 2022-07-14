package main

import (
	"fmt"
	"os"

	"juno-contracts-worker/config"
	"juno-contracts-worker/schema"
	"juno-contracts-worker/schema/mapping"
)

func main() {
	configPath := ""
	args := os.Args[1:]

	for i, a := range args {
		if a == "--config" && i < len(args)-1 {
			configPath = args[i+1]
			break
		}
	}

	config, err := config.ReadConfig(configPath)
	if err != nil {
		fmt.Println("Could not read config: ", err)
		return
	}

	schemaBytes, err := os.ReadFile(config.SchemaPath)
	if err != nil {
		fmt.Println("Could not read schema file: ", err)
		return
	}

	schemaMap := mapping.ParseSchemaToMap(string(schemaBytes))

	if err := schema.GenerateResolvers(schemaMap, config.ResolversPath); err != nil {
		fmt.Println("Could not generate resolvers: ", err)
		return
	}
}
