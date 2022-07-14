package main

import (
	"fmt"
	"juno-contracts-worker/database"
	"juno-contracts-worker/parser"
	s "juno-contracts-worker/schema"
	"juno-contracts-worker/schema/mapping"
	"juno-contracts-worker/server"
	"os"
)

const (
	DB_USER     = "postgres"
	DB_PASSWORD = "postgres"
	DB_NAME     = "postgres"
)

func main() {
	schema, err := os.ReadFile("schema.graphql")
	if err != nil {
		println(err)
		return
	}

	schemaMap := mapping.ParseSchemaToMap(string(schema))

	db, err := database.New(schemaMap, DB_USER, DB_PASSWORD, DB_NAME)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer db.Close()

	// err = db.UpdateStateHeight(3803514)
	h, _ := db.GetStateHeight()
	fmt.Println("Starting with height: ", h)

	parser := parser.New(db, schemaMap)
	if e := parser.StartParsing("msg_instantiate_contracts", 3804888); e != nil {
		fmt.Println("err parsing: ", e)
		os.Exit(1)
	}

	shouldRestart := true

	if s.GenerateResolvers(schemaMap) && shouldRestart {
		os.Exit(2)
	}

	serv := server.New(string(schema), s.NewQuery(db))

	serv.Start()

}
