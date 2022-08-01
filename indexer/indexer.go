package indexer

import (
	"juno-contracts-worker/db"
	"juno-contracts-worker/utils"

	"github.com/google/uuid"
	"github.com/iancoleman/strcase"
	"github.com/sirupsen/logrus"

	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type manyToMany struct {
	tableName string
	field     string
	ids       []string
}

type Service struct {
	db  *db.DB
	log *logrus.Logger
}

func New(d *db.DB, l *logrus.Logger) *Service {
	return &Service{db: d, log: l}
}

func (s *Service) CreateIndex(idxName, parentTableName, tableName string) error {
	s.log.Debugf("Create index %s: %s: %s", idxName, parentTableName, tableName)
	return s.db.CreateIndex(idxName, parentTableName, tableName)
}

func (s *Service) CreateTable(tableName string, fields map[string]interface{}) error {
	s.log.Debugf("Create table %s: %v", tableName, fields)
	return s.db.CreateTable(tableName, fields)
}

func (s *Service) CreateColumns(tableName string, fields map[string]interface{}) error {
	s.log.Debugf("Create columns %s: %v", tableName, fields)

	tableName = utils.UniqueShortName(tableName)

	for k, v := range fields {
		val := v.(string)
		if strings.Contains(k, "UNIQUE") {
			continue

		} else if strings.Contains(k, "REFERENCES") {
			k = utils.GetFieldName(k)
			if err := s.db.CreateIndex(k, tableName, k); err != nil {
				return err
			}

		} else if err := s.db.CreateColumn(tableName, k, val); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) QueryFields(tableName string, fields []string, h *int32, id *string) (*sql.Rows, error) {
	return s.db.Select(tableName, fields, h, id)
}

func (s *Service) TableExists(tableName string) (bool, error) {
	s.log.Debugf("Query table exists: %s", tableName)
	return s.db.TableExists(tableName)
}

func (s *Service) SaveJson(name string, json map[string]interface{}) (string, error) {
	s.log.Debugf("Save entity %s", name)

	uuid, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	entityID := fmt.Sprintf("%v", uuid)
	json["id"] = entityID

	vArray, fields, manies, err := s.parseJsonIntoQuery(json, name)
	if err != nil {
		return "", err
	}

	if err := s.db.Insert(name, vArray, fields); err != nil {
		return "", err
	}

	for _, m := range manies {
		if err = s.saveManyToMany(m.tableName, m.field, entityID, m.ids); err != nil {
			return "", err
		}
	}

	return entityID, nil
}

func (s *Service) parseJsonIntoQuery(json map[string]interface{}, name string) (valuesArr []any, fields []string, m []manyToMany, err error) {
	name = utils.DeleteS(name)
	for k, v := range json {
		k = utils.DeleteS(k)
		field := k

		switch reflect.TypeOf(v) {
		case reflect.TypeOf(map[string]interface{}{}):
			field = strcase.ToSnake(fmt.Sprintf("%s %s", name, k))
			entityID, err := s.SaveJson(field, v.(map[string]interface{}))
			if err != nil {
				return nil, nil, nil, fmt.Errorf("could not save %s, err: %w", field, err)
			}
			valuesArr = append(valuesArr, entityID)
			fields = append(fields, utils.UniqueShortName(field))

		case reflect.TypeOf([]interface{}{}):
			val := v.([]interface{})

			if isArray, _ := utils.IsArray(val); isArray {
				valuesArr = append(valuesArr, mapArray(val))
				fields = append(fields, field)

			} else {
				field = strcase.ToSnake(fmt.Sprintf("%s %s", name, k))

				ids, err := s.saveStructArray(val, field)
				if err != nil {
					return nil, nil, nil, err
				}

				m = append(m, manyToMany{
					tableName: name,
					field:     field,
					ids:       ids,
				})
			}

		case reflect.TypeOf(""), reflect.TypeOf(float64(0)):
			valuesArr = append(valuesArr, v)
			fields = append(fields, field)

		default:
			fmt.Println("unknown type ", reflect.TypeOf(v))
		}
	}

	return valuesArr, fields, m, nil
}

func mapArray(m []interface{}) string {
	s := "{"
	l := len(m)
	for i := 0; i < l; i++ {
		s += mapArrayToString(m[i].(map[string]interface{}))
		if i < l-1 {
			s += ", "
		}
	}
	s += "}"
	return s
}

func mapArrayToString(m map[string]interface{}) (s string) {
	l := len(m)
	for i := 0; i < l; i++ {
		value := m[strconv.Itoa(i)]
		valueType := reflect.TypeOf(value)
		switch valueType.String() {
		case "String":
			s += value.(string)
		case "Boolean":
			boolean, _ := value.(bool)
			s += strconv.FormatBool(boolean)
		default:
			fmt.Println("uknown value type ", valueType)
		}
	}
	return s
}

func (s *Service) saveStructArray(arr []interface{}, fieldName string) (ids []string, err error) {
	for i := 0; i < len(arr); i++ {
		entityID, err := s.SaveJson(fieldName, arr[i].(map[string]interface{}))
		if err != nil {
			return nil, fmt.Errorf("could not save %s, err: %w", fieldName, err)
		}
		ids = append(ids, entityID)
	}

	return ids, nil
}

func (s *Service) saveManyToMany(table, field string, entityID string, ids []string) (err error) {
	for _, id := range ids {
		uuid, err := uuid.NewRandom()
		if err != nil {
			return err
		}

		if strings.Contains(table, "_") {
			table = utils.UniqueShortName(table)
		}
		f := utils.UniqueShortName(field)

		s.log.Debugf("Save many to many %s and %s ", table, field)

		if err = s.db.Insert(field+"_r", []any{uuid, entityID, id}, []string{"id", table, f}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) LinkTable(id, linkID, idxName, tableName string) error {
	s.log.Debugf("Link table %s with %s", tableName, idxName)
	return s.db.LinkTable(id, linkID, idxName, tableName)
}
