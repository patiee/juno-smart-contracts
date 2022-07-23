package sync

import (
	"fmt"

	"juno-contracts-worker/db"
)

const tableName = "sync_height"

type Service struct {
	db *db.DB
}

func New(db *db.DB) (*Service, error) {
	s := &Service{
		db: db,
	}

	return s, s.initSyncHeightTable()
}

func (s *Service) initSyncHeightTable() error {
	tableFields := map[string]interface{}{
		"height": "INT",
	}

	if err := s.db.CreateTable(tableName, tableFields); err != nil {
		return fmt.Errorf("could not create table %s: %w", tableName, err)
	}

	return nil
}

func (s *Service) GetSyncHeight() (height int32, err error) {
	rows, err := s.db.Select(tableName, []string{"height"}, nil, nil)
	if err != nil {
		return 0, err
	}

	for rows.Next() {
		if err = rows.Scan(&height); err != nil {
			return 0, err
		}
	}

	return height, nil
}

func (s *Service) UpdateSyncHeight(height int32) error {
	return s.db.Update(tableName, "height", fmt.Sprint(height))
}
