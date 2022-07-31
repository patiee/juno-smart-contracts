package db

import (
	"database/sql"
	"fmt"
	"juno-contracts-worker/utils"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type DB struct {
	conn *sql.DB
}

func New(user, password, dbName string) (*DB, error) {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		user, password, dbName)

	conn, err := sql.Open("postgres", dbinfo)
	if err != nil {
		return nil, fmt.Errorf("could not connect with database: %w", err)
	}

	return &DB{conn: conn}, nil
}

func (db *DB) Close() {
	db.conn.Close()
}

func (db *DB) CreateTable(tableName string, fields Fields) error {
	q := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS app.%s (
		id UUID PRIMARY KEY%s
	);`, utils.UniqueShortName(tableName), fields.CreateTableString())

	fmt.Printf("Create table %s: query: %s", tableName, q)

	if _, err := db.conn.Exec(q); err != nil {
		return err
	}

	time.Sleep(500)

	return nil
}

func (db *DB) CreateColumn(tableName, columnName, columnType string) error {
	q := fmt.Sprintf(`ALTER TABLE app.%s ADD COLUMN IF NOT EXISTS %s %s;`,
		utils.UniqueShortName(tableName), columnName, columnType)

	fmt.Printf("Add column to table %s: query: %s\n", tableName, q)

	if _, err := db.conn.Exec(q); err != nil {
		fmt.Println("could not add column: ", err)
		return err
	}

	time.Sleep(500)

	return nil
}

func (db *DB) Select(tableName string, fields []string, height *int32, id *string) (*sql.Rows, error) {
	whereQ := "WHERE "
	if height != nil && *height != 0 {
		whereQ += fmt.Sprintf("height = %d ", *height)
	}

	if id != nil && *id != "" {
		whereQ += fmt.Sprintf("id = '%s' ", *id)
	}

	if len(whereQ) < 8 {
		whereQ = ""
	}

	q := fmt.Sprintf("SELECT %s FROM app.%s %s;", strings.Join(fields, ", "), tableName, whereQ)

	fmt.Println("Select: ", q)

	return db.conn.Query(q)
}

func (db *DB) Update(tableName string, fieldName, fieldValue string) error {
	q := fmt.Sprintf("UPDATE app.%s SET %s=%s;", tableName, fieldName, fieldValue)

	fmt.Println("Update: ", q)

	_, err := db.conn.Exec(q)
	return err
}

func (db *DB) TableExists(tableName string) (bool, error) {
	var str string
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf("SELECT to_regclass('app.%s');", tableName)

	fmt.Println("Table exists: ", q)

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

func (db *DB) CreateIndex(idxName, parentTableName, tableName string) error {
	idxName = utils.UniqueShortName(idxName)
	tableName = utils.UniqueShortName(tableName)
	q := fmt.Sprintf(`ALTER TABLE app.%s ADD COLUMN IF NOT EXISTS %s UUID REFERENCES app.%s;`,
		parentTableName, idxName, tableName)
	fmt.Println("Create index: ", q)
	_, err := db.conn.Exec(q)
	return err
}

func (db *DB) Insert(tableName string, values []any, fieldNames []string) error {
	tableName = utils.UniqueShortName(tableName)
	fieldNames = utils.AddUnderscoresIfMissing(fieldNames)

	q := fmt.Sprintf(`INSERT INTO app.%s (%s) VALUES (%s)`,
		tableName, strings.Join(fieldNames, ", "), printValueNames(len(fieldNames)))

	fmt.Println("insert: ", q)
	fmt.Println("values: ", values)

	if _, err := db.conn.Exec(q, values...); err != nil {
		err = fmt.Errorf("could not insert into database, err: %w", err)
		fmt.Println("err: ", err)
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
	q := fmt.Sprintf(`UPDATE app.%s SET %s='%s' WHERE id='%s'`,
		tableName, idxName, linkID, id)

	fmt.Println("Link: ", q)
	if _, err := db.conn.Exec(q); err != nil {
		return err
	}

	return nil
}

type Fields map[string]interface{}

func (f *Fields) CreateTableString() string {
	if len(*f) == 0 {
		return ""
	}

	s := ",\n"

	for k, v := range *f {
		str := v.(string)
		if strings.Contains(str, "REFERENCE") {
			k = utils.UniqueShortName(k)
		}
		s += fmt.Sprintf("%s %s,\n", k, v)
	}
	return s[0 : len(s)-2]
}
