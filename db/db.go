package db

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/iancoleman/strcase"
	_ "github.com/lib/pq"
	"github.com/pborman/uuid"
)

type DB struct {
	conn *sql.DB
}

func New(user, password, dbName string) (db *DB, err error) {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		user, password, dbName)

	db.conn, err = sql.Open("postgres", dbinfo)
	if err != nil {
		return nil, fmt.Errorf("could not connect with database: %w", err)
	}

	return db, nil
}

func (db *DB) Close() {
	db.conn.Close()
}
func (db *DB) Query(tableName string, fields []string, height *int32, id *string) (*sql.Rows, error) {
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

	q := fmt.Sprintf("SELECT %s FROM app.%s %s", strings.Join(fields, ","), tableName, whereQ)

	rows, err := db.conn.Query(q)
	if err != nil {
		fmt.Println("error: ", err)
		return nil, errors.New(fmt.Sprintf("Could not query %s: %s", tableName, err))
	}
	return rows, nil
}

func (db *DB) CreateIndex(idxName, parentTableName, tableName string) error {
	createIndex := fmt.Sprintf(`ALTER TABLE app.%s ADD COLUMN IF NOT EXISTS %s TEXT REFERENCES app.%s;`, parentTableName, idxName, tableName)
	_, err := db.conn.Exec(createIndex)
	return err
}

func (db *DB) CreateTable3(schema map[string]interface{}, name string) error {
	indexesStr, err := db.printIndexesAndCreateNestedTables(schema, name)
	if err != nil {
		return err
	}

	q := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS app.%s (
		id TEXT PRIMARY KEY %s
	);`, name, indexesStr)

	fmt.Println("Create table query: \n", q)

	if _, err = db.conn.Exec(q); err != nil {
		return err
	}
	return err
}

func (db *DB) printIndexesAndCreateNestedTables(schema map[string]interface{}, name string) (string, error) {
	indexes := ""
	for k, v := range schema {
		if k == "id" {
			continue
		}

		field, value := strcase.ToSnake(k), ""

		switch reflect.TypeOf(v) {
		case reflect.TypeOf(map[string]interface{}{}):
			nameSneak := strcase.ToSnake(k)
			if err := db.CreateTable(v.(map[string]interface{}), nameSneak); err != nil {
				return "", err
			}

			field = strcase.ToSnake(field)
			value = fmt.Sprintf("TEXT REFERENCES app.%s", nameSneak)
		case reflect.TypeOf(""):
			valueString := v.(string)
			if string(valueString[0]) == "[" {
				name := valueString[1 : len(valueString)-1]
				if err := db.CreateTable(db.schema[name].(map[string]interface{}), strcase.ToSnake(name)); err != nil {
					return "", err
				}
			}

			value = getDatabaseType(valueString)

		default:
			fmt.Println("Unhandled value type to process! ", reflect.TypeOf(v))
		}

		indexes += fmt.Sprintf(",\n\t%s %s", field, value)
	}
	return indexes, nil
}

func getDatabaseType(val string) (dbType string) {
	switch val {
	case "String":
		dbType = "TEXT"
	case "Int":
		dbType = "INT"
	default:
		dbType = "TEXT"
		if string(val[0]) == "[" {
			dbType += "[]"
		}
	}
	return
}

func isReference(val string) bool {
	switch val {
	case "String", "Int":
		return false
	default:
		return true
	}
}

func (db *DB) InsertJsonIntoTable(json map[string]interface{}, name string) (string, error) {
	entityID := fmt.Sprintf("%v%d", uuid.NewRandom(), rand.Int63n(time.Now().Unix()))
	json["id"] = entityID

	vArray, fields, values, err := db.parseJsonIntoQuery(json, name)
	if err != nil {
		return "", nil
	}

	q := fmt.Sprintf(`INSERT INTO app.%s (%s) VALUES (%s)`, name, fields, values)

	if _, err := db.conn.Exec(q, vArray...); err != nil {
		fmt.Println("InsertJsonIntoTable Error: ", err)
		return "", errors.New(fmt.Sprintf("Could not save into database, err: %s", err))
	}

	return entityID, nil
}

func (db *DB) parseJsonIntoQuery(json map[string]interface{}, name string) (valuesArr []any, fields string, values string, err error) {
	count := 1
	for k, v := range json {
		field := fmt.Sprintf("%s, ", k)
		values += fmt.Sprintf("$%d, ", count)
		switch reflect.TypeOf(v) {
		case reflect.TypeOf(map[string]interface{}{}):
			entityID, err := db.InsertJsonIntoTable(v.(map[string]interface{}), strcase.ToSnake(fmt.Sprintf("%s %s", name, k)))
			if err != nil {
				return nil, "", "", err
			}

			valuesArr = append(valuesArr, entityID)
			field = fmt.Sprintf("%s, ", strcase.ToSnake(fmt.Sprintf("%s %s", name, k)))

		case reflect.TypeOf([]interface{}{}):
			var ids []string
			val := v.([]interface{})
			l := len(val)
			for i := 0; i < l; i++ {
				entityID, err := db.InsertJsonIntoTable(val[i].(map[string]interface{}), strcase.ToSnake(fmt.Sprintf("%s %s", name, k)))
				if err != nil {
					return nil, "", "", err
				}
				ids = append(ids, fmt.Sprintf("'%s'", entityID))
			}

			valuesArr = append(valuesArr, fmt.Sprintf("{%s}", strings.Join(ids, ",")))
			field = fmt.Sprintf("%s, ", strcase.ToSnake(fmt.Sprintf("%s %s", name, k)))

		case reflect.TypeOf(""), reflect.TypeOf(float64(0)):
			valuesArr = append(valuesArr, v)

		default:
			fmt.Println("unknown type!!! ", reflect.TypeOf(v))
		}
		fields += field
		count++
	}

	fields = fields[0 : len(fields)-2]
	values = values[0 : len(values)-2]
	return valuesArr, fields, values, nil
}

func (db *DB) LinkTable(id, linkID, idxName, tableName string) error {
	q := fmt.Sprintf(`UPDATE app.%s SET %s='%s' WHERE id='%s'`, tableName, idxName, linkID, id)

	fmt.Println("link query: ", q)
	if _, err := db.conn.Exec(q); err != nil {
		return err
	}

	return nil
}

type Fields map[string]interface{}

func (f *Fields) CreateTableString() (s string) {
	for k, v := range *f {
		s += fmt.Sprintf("%s %s,\n", k, v)
	}
	return s[0 : len(s)-3]
}

func (f *Fields) SelectString() (s string) {
	for k := range *f {
		s += fmt.Sprintf("%s, ", k)
	}
	return s[0 : len(s)-2]
}
