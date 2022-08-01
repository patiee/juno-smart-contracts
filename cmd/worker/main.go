package main

import (
	"fmt"
	"os"

	"juno-contracts-worker/client"
	"juno-contracts-worker/config"
	"juno-contracts-worker/db"
	"juno-contracts-worker/indexer"
	"juno-contracts-worker/sync"
	"juno-contracts-worker/worker"

	"github.com/sirupsen/logrus"
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

	log := &logrus.Logger{
		Out:       os.Stdout,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}

	db, err := db.New(log, config.DbUser, config.DbPassword, config.DbName)
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

	grpcClient, err := client.New(config.GrpcUrl, log)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer grpcClient.Close()

	worker := worker.New(grpcClient, indexer.New(db, log), log, sync)
	// if e := worker.Start("msg_instantiate_contracts", 3803514); e != nil {
	// 	fmt.Println("Error while processing data: ", err)
	// 	return
	// }
	if e := worker.Start("msg_execute_contracts", 4052931); e != nil {
		fmt.Println("Error while processing data: ", err)
		return
	}

}
