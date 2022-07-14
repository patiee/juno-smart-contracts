package main

import (
	"fmt"
	"os"

	"juno-contracts-worker/config"
	"juno-contracts-worker/database"
	"juno-contracts-worker/schema"
	"juno-contracts-worker/schema/mapping"
	"juno-contracts-worker/server"
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

	db, err := database.New(schemaMap, config.DB_User, config.DB_Password, config.DB_Name)
	if err != nil {
		fmt.Println("Could not connect with database: ", err)
		return
	}
	defer db.Close()

	serv := server.New(string(schemaBytes), schema.NewQuery(db))

	serv.Start()
}
