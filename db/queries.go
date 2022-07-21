package db

import (
	"database/sql"
	"fmt"
)

func (db *DB) CreateTable(tableName string, fields Fields) error {
	createTable := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS app.%s (
		id UUID PRIMARY KEY,
		%s
	);`, tableName, fields.CreateTableString())

	if _, err := db.conn.Exec(createTable); err != nil {
		return err
	}

	return nil
}

func (db *DB) Select(tableName string, fields Fields) (*sql.Rows, error) {
	q := fmt.Sprintf("SELECT %s FROM app.%s;", fields.SelectString(), tableName)

	return db.conn.Query(q)
}

func (db *DB) Update(tableName string, fieldName, fieldValue string) error {
	q := fmt.Sprintf(`UPDATE app.%s SET %s=%s;`, tableName, fieldName, fieldValue)

	_, err := db.conn.Exec(q)
	return err
}
