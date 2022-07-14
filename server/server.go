package server

import (
	"log"
	"net/http"

	graphql "github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
)

type Server struct {
	schema    string
	resolvers interface{}
}

func New(schema string, resolvers interface{}) *Server {
	return &Server{
		schema:    schema,
		resolvers: resolvers,
	}
}

func (s *Server) Start() {
	graphqlSchema := graphql.MustParseSchema(s.schema, s.resolvers)
	http.Handle("/query", &relay.Handler{Schema: graphqlSchema})
	go log.Fatal(http.ListenAndServe(":8080", nil))
}
