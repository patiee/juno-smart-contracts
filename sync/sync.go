package sync

import (
	"fmt"

	"juno-contracts-worker/db"
)

type Service struct {
	tableName   string
	tableFields map[string]interface{}

	db *db.DB
}

func New(db *db.DB) (*Service, error) {
	s := &Service{
		tableName: "sync_height",
		tableFields: map[string]interface{}{
			"height": "INT",
		},

		db: db,
	}

	return s, s.initSyncHeightTable()
}

func (s *Service) initSyncHeightTable() error {
	if err := s.db.CreateTable(s.tableName, s.tableFields); err != nil {
		return fmt.Errorf("could not create table %s: %w", s.tableName, err)
	}

	return nil
}

func (s *Service) GetSyncHeight() (height int32, err error) {
	rows, err := s.db.Select(s.tableName, s.tableFields)

	for rows.Next() {
		if err = rows.Scan(&height); err != nil {
			return 0, err
		}
	}

	return height, nil
}

func (s *Service) UpdateSyncHeight(height int32) error {
	return s.db.Update(s.tableName, "height", fmt.Sprint(height))
}
