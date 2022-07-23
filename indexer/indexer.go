package indexer

import (
	"database/sql"
	"fmt"
	"juno-contracts-worker/db"
	"juno-contracts-worker/utils"
	"reflect"

	"github.com/google/uuid"
	"github.com/iancoleman/strcase"
)

type manyToMany struct {
	tableName string
	field     string
	ids       []string
}

type Service struct {
	db *db.DB
}

func New(db *db.DB) *Service {
	return &Service{db: db}
}

func (s *Service) CreateIndex(idxName, parentTableName, tableName string) error {
	return s.db.CreateIndex(idxName, parentTableName, tableName)
}

func (s *Service) CreateTable(tableName string, fields map[string]interface{}) error {
	return s.db.CreateTable(tableName, fields)
}

func (s *Service) QueryFields(tableName string, fields []string, h *int32, id *string) (*sql.Rows, error) {
	return s.db.Select(tableName, fields, h, id)
}

func (s *Service) TableExists(tableName string) (bool, error) {
	return s.db.TableExists(tableName)
}

func (s *Service) SaveJson(name string, json map[string]interface{}) (string, error) {
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
			fields = append(fields, field)

		case reflect.TypeOf([]interface{}{}):
			var ids []string
			val := v.([]interface{})
			l := len(val)
			field = strcase.ToSnake(fmt.Sprintf("%s %s", name, k))

			for i := 0; i < l; i++ {
				entityID, err := s.SaveJson(field, val[i].(map[string]interface{}))
				if err != nil {
					return nil, nil, nil, fmt.Errorf("could not save %s, err: %w", field, err)
				}
				ids = append(ids, entityID)
			}

			m = append(m, manyToMany{
				tableName: name,
				field:     field,
				ids:       ids,
			})

		case reflect.TypeOf(""), reflect.TypeOf(float64(0)):
			valuesArr = append(valuesArr, v)
			fields = append(fields, field)

		default:
			fmt.Println("unknown type ", reflect.TypeOf(v))
		}

	}

	return valuesArr, fields, m, nil
}

func (s *Service) saveManyToMany(table, field string, entityID string, ids []string) (err error) {
	for _, id := range ids {
		uuid, err := uuid.NewRandom()
		if err != nil {
			return err
		}

		if err = s.db.Insert(field+"_r", []any{uuid, entityID, id}, []string{"id", table, field}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) LinkTable(id, linkID, idxName, tableName string) error {
	return s.db.LinkTable(id, linkID, idxName, tableName)
}
