package db

import (
	"database/sql"
	"juno-contracts-worker/db/model"
)

const maxConnections = 98

type ServiceLimiter struct {
	conn chan struct{}
	db   ServiceInterface
}

func NewServiceWithConnectionLimiter(db ServiceInterface) ServiceInterface {
	return &ServiceLimiter{
		conn: make(chan struct{}, maxConnections),
		db:   db,
	}
}

func (s *ServiceLimiter) Close() {
	s.db.Close()
}

func (s *ServiceLimiter) CreateTable(tableName string, fields model.Fields) error {
	s.conn <- struct{}{}
	defer func() {
		<-s.conn
	}()
	return s.db.CreateTable(tableName, fields)
}

func (s *ServiceLimiter) CreateColumn(tableName, columnName, columnType string) error {
	s.conn <- struct{}{}
	defer func() {
		<-s.conn
	}()
	return s.db.CreateColumn(tableName, columnName, columnType)
}

func (s *ServiceLimiter) Select(tableName string, fields []string, qParams *model.QParameters) (*sql.Rows, error) {
	s.conn <- struct{}{}
	defer func() {
		<-s.conn
	}()
	return s.db.Select(tableName, fields, qParams)
}

func (s *ServiceLimiter) Update(tableName string, qParams model.QParameters, fields map[string]string) error {
	s.conn <- struct{}{}
	defer func() {
		<-s.conn
	}()
	return s.db.Update(tableName, qParams, fields)
}

func (s *ServiceLimiter) TableExists(tableName string) (bool, error) {
	s.conn <- struct{}{}
	defer func() {
		<-s.conn
	}()
	return s.db.TableExists(tableName)
}

func (s *ServiceLimiter) CreateUniqueIndex(columns []string, indexName, tableName string) error {
	s.conn <- struct{}{}
	defer func() {
		<-s.conn
	}()
	return s.db.CreateUniqueIndex(columns, indexName, tableName)
}

func (s *ServiceLimiter) AddColumn(idxName, parentTableName, tableName string) error {
	s.conn <- struct{}{}
	defer func() {
		<-s.conn
	}()
	return s.db.AddColumn(idxName, parentTableName, tableName)
}

func (s *ServiceLimiter) Insert(tableName string, fieldNames []string, values []any) error {
	s.conn <- struct{}{}
	defer func() {
		<-s.conn
	}()
	return s.db.Insert(tableName, fieldNames, values)
}

func (s *ServiceLimiter) LinkTable(id, linkID, idxName, tableName string) error {
	s.conn <- struct{}{}
	defer func() {
		<-s.conn
	}()
	return s.db.LinkTable(id, linkID, idxName, tableName)
}
