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

type DB struct {
	conn *sql.DB
	log  *logrus.Logger
}

func New(log *logrus.Logger, user, password, dbName string) (*DB, error) {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		user, password, dbName)

	conn, err := sql.Open("postgres", dbinfo)
	if err != nil {
		return nil, fmt.Errorf("could not connect with database: %w", err)
	}

	return &DB{
		conn: conn,
		log:  log,
	}, nil
}

func (db *DB) Close() {
	db.log.Debug("Close database connection")
	db.conn.Close()
}

func (db *DB) CreateTable(tableName string, fields model.Fields) error {
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS app.%s (
		id UUID PRIMARY KEY%s
	);`, tableName, fields.CreateTableString())

	db.log.Debugf("Create table %s query: %s", tableName, q)

	if _, err := db.conn.Exec(q); err != nil {
		return err
	}

	return nil
}

func (db *DB) CreateColumn(tableName, columnName, columnType string) error {
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf(`ALTER TABLE app.%s ADD COLUMN IF NOT EXISTS %s %s;`,
		tableName, columnName, columnType)

	db.log.Debugf("Add column to table %s query: %s", tableName, q)

	if _, err := db.conn.Exec(q); err != nil {
		fmt.Println("could not add column: ", err)
		return err
	}

	return nil
}

func (db *DB) Select(tableName string, fields []string, qParams *model.QParameters) (*sql.Rows, error) {
	q := fmt.Sprintf("SELECT %s FROM app.%s %s;",
		strings.Join(fields, ", "), tableName, qParams.Print())

	db.log.Debugf("Select query: %s", q)
	return db.conn.Query(q)
}

func (db *DB) Update(qParams model.QParameters, tableName, fieldName, fieldValue string) error {
	q := fmt.Sprintf("UPDATE app.%s SET %s=%s %s;",
		tableName, fieldName, fieldValue, qParams.Print())

	db.log.Debugf("Update query: %s", q)

	_, err := db.conn.Exec(q)
	return err
}

func (db *DB) TableExists(tableName string) (bool, error) {
	var str string
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf("SELECT to_regclass('app.%s');",
		tableName)

	rows, err := db.conn.Query(q)
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

func (db *DB) CreateUniqueIndex(columns []string, indexName, tableName string) error {
	indexName = utils.UniqueShortName(indexName)
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s ON app.%s(%s);`,
		indexName, tableName, strings.Join(columns, ", "))

	db.log.Debugf("Create unique index query: %s", q)
	_, err := db.conn.Exec(q)
	return err
}

func (db *DB) AddColumn(idxName, parentTableName, tableName string) error {
	idxName = utils.UniqueShortName(idxName)
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf(`ALTER TABLE app.%s ADD COLUMN IF NOT EXISTS %s UUID REFERENCES app.%s;`,
		parentTableName, idxName, tableName)

	db.log.Debugf("Create index query: %s", q)
	_, err := db.conn.Exec(q)
	return err
}

func (db *DB) Insert(tableName string, fieldNames []string, values []any) error {
	tableName = utils.UniqueShortName(tableName)
	fieldNames = utils.AddUnderscoresIfMissing(fieldNames)

	q := fmt.Sprintf(`INSERT INTO app.%s (%s) VALUES (%s) ON CONFLICT DO NOTHING;`,
		tableName, strings.Join(fieldNames, ", "), printValueNames(len(fieldNames)))

	if _, err := db.conn.Exec(q, values...); err != nil {
		err = fmt.Errorf("could not insert into database, err: %w", err)
		db.log.Error(err)
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

func (db *DB) LinkTable(id, linkID, idxName, tableName string) error {
	idxName = utils.UniqueShortName(idxName)
	q := fmt.Sprintf(`UPDATE app.%s SET %s='%s' WHERE id='%s';`,
		tableName, idxName, linkID, id)

	db.log.Debugf("Link query: ", q)
	if _, err := db.conn.Exec(q); err != nil {
		return err
	}

	return nil
}
