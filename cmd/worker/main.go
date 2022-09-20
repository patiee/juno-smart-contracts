package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/sirupsen/logrus"

	"juno-contracts-worker/client"
	"juno-contracts-worker/config"
	"juno-contracts-worker/db"
	"juno-contracts-worker/indexer"
	"juno-contracts-worker/utils"
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

	log := &logrus.Logger{
		Out:       os.Stdout,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     utils.LogLevel(config.LogLevel),
	}

	dbService, err := db.New(log, config.DbUser, config.DbPassword, config.DbName)
	if err != nil {
		fmt.Println("Could not connect with database: ", err)
		return
	}
	dbWithLimiter := db.NewServiceWithConnectionLimiter(dbService)
	defer dbWithLimiter.Close()

	grpcClient, err := client.New(config.GrpcUrl, log)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer grpcClient.Close()

	indexer := indexer.New(grpcClient, dbWithLimiter, log)

	worker, err := worker.New(dbWithLimiter, log, indexer)
	if err != nil {
		fmt.Println("Error while creating sync: ", err)
		return
	}

	var wg sync.WaitGroup
	for _, msg := range config.Messages {
		wg.Add(1)
		go worker.StartSync(&wg, msg)
	}
	wg.Wait()
}
