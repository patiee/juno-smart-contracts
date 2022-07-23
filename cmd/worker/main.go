package main

import (
	"fmt"
	"os"

	"juno-contracts-worker/config"
	"juno-contracts-worker/db"
	"juno-contracts-worker/indexer"
	"juno-contracts-worker/sync"
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

	db, err := db.New(config.DB_User, config.DB_Password, config.DB_Name)
	if err != nil {
		fmt.Println("Could not connect with database: ", err)
		return
	}
	defer db.Close()

	// err = db.UpdateStateHeight(3803514)
	// h, _ := db.GetStateHeight()
	// fmt.Println("Starting with height: ", h)

	sync, err := sync.New(db)
	if err != nil {
		fmt.Println("Error while creating sync: ", err)
		return
	}

	worker := worker.New(indexer.New(db), sync)
	if e := worker.Start("msg_instantiate_contracts", 3807889); e != nil {
		fmt.Println("Error while processing data: ", err)
		return
	}

}
