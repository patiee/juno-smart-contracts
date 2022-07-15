# Juno Smart Contracts 
This worker was builded to process json from CosmWasm contracts. For each new data structure creates tables in database and update schema. 


As first step you need to run [SubQuery indexer. ](https://github.com/ogb-interchain/juno-dao-contracts/tree/juno-cosmwasm-contracts)


Worker is using SubQuery database to fetch contract messages:
```
go run cmd/worker/main.go --config config.json
```

Resolvers for graphql query are generated with this command:
```
go run cmd/resolvers/main.go --config config.json
```

GraphQL server needs to be restarted if we want to have newest changes:
```
go run cmd/server/main.go --config config.json
```


API is hosted on: http://localhost:8080/query
example query:
```
query {
	msgInstantiateContract(height: 3804888){
		index
		height
		txHash
		msg
 		msgInstantiateContract452{
 			symbol
 		}
	}
```
