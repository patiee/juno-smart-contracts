package indexer

import (
	"database/sql"
	"juno-contracts-worker/db"
)

type Service struct {
	db *db.DB
}

func New(db *db.DB) *Service {
	return &Service{db: db}
}

func (s *Service) QueryMessage(tableName string) (*sql.Rows, error) {
	var id, txHash, msg string
	var index, h int32 = 0, height
	fields := []string{"id", "index", "tx_hash", "msg"}

	rows, err := s.db.Query(tableName, fields, &h, nil)
	if err != nil {
		return err
	}
}
