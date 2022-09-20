# Juno Smart Contracts 
This worker is processing data from CosmWasm contracts to create new database tables for each new data structure that can be inside transaction messages.

## Subquery indexer
Start indexing transaction messages with [SubQuery indexer. ](https://github.com/ogb-interchain/juno-dao-contracts/tree/juno-cosmwasm-contracts)


## Worker
Before run, make sure that you have address for juno grpc server. You can setup own node with [docker](https://docs.junonetwork.io/smart-contracts-and-junod-development/junod-local-dev-setup#run-juno). Please note that you need to sync node first to height you want to query.

Run worker to process transaction messages:
```
go run cmd/worker/main.go --config config.json
```

## Graphql server
Host [Graphql server](https://github.com/patiee/juno-contracts-indexer) and watch for new entities

