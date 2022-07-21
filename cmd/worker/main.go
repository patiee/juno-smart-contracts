package main

import (
	"fmt"
	"os"

	"juno-contracts-worker/config"
	"juno-contracts-worker/db"
	"juno-contracts-worker/worker"
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

	schema, err := os.ReadFile(config.SchemaPath)
	if err != nil {
		println(err)
		return
	}

	db, err := db.New(config.DB_User, config.DB_Password, config.DB_Name)
	if err != nil {
		fmt.Println("Could not connect with database: ", err)
		return
	}
	defer db.Close()

	// err = db.UpdateStateHeight(3803514)
	// h, _ := db.GetStateHeight()
	// fmt.Println("Starting with height: ", h)

	worker := worker.New(db)
	if e := worker.StartParsing("msg_instantiate_contracts", 3803514); e != nil {
		fmt.Println("Error while processing data: ", err)
		return
	}

}
