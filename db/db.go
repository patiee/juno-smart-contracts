package db

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"juno-contracts-worker/db/model"
	"juno-contracts-worker/utils"
)

type ServiceInterface interface {
	Close()
	CreateTable(tableName string, fields model.Fields) error
	CreateColumn(tableName, columnName, columnType string) error
	Select(tableName string, fields []string, qParams *model.QParameters) (*sql.Rows, error)
	Update(tableName string, qParams model.QParameters, fields map[string]string) error
	TableExists(tableName string) (bool, error)
	CreateUniqueIndex(columns []string, indexName, tableName string) error
	AddColumn(idxName, parentTableName, tableName string) error
	Insert(tableName string, fieldNames []string, values []any) error
	LinkTable(id, linkID, idxName, tableName string) error
}

type Service struct {
	conn *sql.DB
	log  *logrus.Logger
}

func New(log *logrus.Logger, user, password, dbName string) (ServiceInterface, error) {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		user, password, dbName)

	conn, err := sql.Open("postgres", dbinfo)
	if err != nil {
		return nil, fmt.Errorf("could not connect with database: %w", err)
	}

	return &Service{
		conn: conn,
		log:  log,
	}, nil
}

func (s *Service) Close() {
	s.log.Debug("Close database connection")
	s.conn.Close()
}

func (s *Service) CreateTable(tableName string, fields model.Fields) error {
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS app.%s (
		id UUID PRIMARY KEY%s
	);`, tableName, fields.CreateTableString())

	s.log.Debugf("Create table %s query: %s", tableName, q)

	if _, err := s.conn.Exec(q); err != nil {
		return err
	}

	return nil
}

func (s *Service) CreateColumn(tableName, columnName, columnType string) error {
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf(`ALTER TABLE app.%s ADD COLUMN IF NOT EXISTS %s %s;`,
		tableName, columnName, columnType)

	s.log.Debugf("Add column to table %s query: %s", tableName, q)

	if _, err := s.conn.Exec(q); err != nil {
		fmt.Println("could not add column: ", err)
		return err
	}

	return nil
}

func (s *Service) Select(tableName string, fields []string, qParams *model.QParameters) (*sql.Rows, error) {
	q := fmt.Sprintf("SELECT %s FROM app.%s %s;",
		strings.Join(fields, ", "), tableName, qParams.Print())

	s.log.Debugf("Select query: %s", q)
	return s.conn.Query(q)
}

func (s *Service) Update(tableName string, qParams model.QParameters, fields map[string]string) error {
	updateFields := []string{}
	for k, v := range fields {
		updateFields = append(updateFields, fmt.Sprintf(`%s=%s`, k, v))
	}
	q := fmt.Sprintf("UPDATE app.%s SET %s %s;",
		tableName, strings.Join(updateFields, ", "), qParams.Print())

	s.log.Debugf("Update query: %s", q)

	_, err := s.conn.Exec(q)
	return err
}

func (s *Service) TableExists(tableName string) (bool, error) {
	var str string
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf("SELECT to_regclass('app.%s');",
		tableName)

	rows, err := s.conn.Query(q)
	if err != nil {
		return false, err
	}

	for rows.Next() {
		if err = rows.Scan(&str); err != nil {
			// if table exists then value is NULL and we got error here
			return false, nil
		}
	}

	return true, nil
}

func (s *Service) CreateUniqueIndex(columns []string, indexName, tableName string) error {
	indexName = utils.UniqueShortName(indexName)
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s ON app.%s(%s);`,
		indexName, tableName, strings.Join(columns, ", "))

	s.log.Debugf("Create unique index query: %s", q)
	_, err := s.conn.Exec(q)
	return err
}

func (s *Service) AddColumn(idxName, parentTableName, tableName string) error {
	idxName = utils.UniqueShortName(idxName)
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf(`ALTER TABLE app.%s ADD COLUMN IF NOT EXISTS %s UUID REFERENCES app.%s;`,
		parentTableName, idxName, tableName)

	s.log.Debugf("Create index query: %s", q)
	_, err := s.conn.Exec(q)
	return err
}

func (s *Service) Insert(tableName string, fieldNames []string, values []any) error {
	tableName = utils.UniqueShortName(tableName)
	fieldNames = utils.AddUnderscoresIfMissing(fieldNames)

	q := fmt.Sprintf(`INSERT INTO app.%s (%s) VALUES (%s) ON CONFLICT DO NOTHING;`,
		tableName, strings.Join(fieldNames, ", "), printValueNames(len(fieldNames)))

	if _, err := s.conn.Exec(q, values...); err != nil {
		err = fmt.Errorf("could not insert into database, err: %w", err)
		s.log.Error(err)
		return err
	}

	return nil
}

func printValueNames(len int) string {
	values := make([]string, len)
	for i := 1; i < len+1; i++ {
		values[i-1] = fmt.Sprintf("$%d", i)
	}
	return strings.Join(values, ", ")
}

func (s *Service) LinkTable(id, linkID, idxName, tableName string) error {
	idxName = utils.UniqueShortName(idxName)
	q := fmt.Sprintf(`UPDATE app.%s SET %s='%s' WHERE id='%s';`,
		tableName, idxName, linkID, id)

	s.log.Debugf("Link query: ", q)
	if _, err := s.conn.Exec(q); err != nil {
		return err
	}

	return nil
}
