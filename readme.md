# Juno Smart Contracts 
This worker is processing data from CosmWasm contracts to create new database tables for each new data structure that can be inside transaction messages.

## Subquery indexer
Start indexing transaction messages with [SubQuery indexer. ](https://github.com/ogb-interchain/juno-dao-contracts/tree/juno-cosmwasm-contracts)


## Worker
Run worker to process transaction messages:
```
go run cmd/worker/main.go --config config.json
```

## Graphql server
Host [Graphql server](https://github.com/patiee/juno-contracts-indexer) and watch for new entities

