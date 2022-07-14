package schema

import (
	db "juno-contracts-worker/database"
)

type Query struct {
	db *db.DB
}

type Parameters struct {
	ID     *string
	Height *int32
}

func NewQuery(db *db.DB) *Query {
	return &Query{
		db: db,
	}
}
