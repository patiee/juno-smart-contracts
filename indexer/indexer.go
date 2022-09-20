package indexer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/iancoleman/strcase"
	"github.com/sirupsen/logrus"

	"juno-contracts-worker/client"
	"juno-contracts-worker/db"
	"juno-contracts-worker/db/model"
	"juno-contracts-worker/utils"
)

type manyToMany struct {
	tableName string
	field     string
	ids       []string
}

type Service struct {
	client *client.Client
	db     db.ServiceInterface
	log    *logrus.Logger
}

func New(c *client.Client, d db.ServiceInterface, l *logrus.Logger) *Service {
	return &Service{client: c, db: d, log: l}
}

func (s *Service) AddColumn(idxName, parentTableName, tableName string) error {
	s.log.Debugf("Add column %s: %s: %s", idxName, parentTableName, tableName)
	return s.db.AddColumn(idxName, parentTableName, tableName)
}

func (s *Service) CreateTable(tableName string, fields map[string]interface{}) error {
	s.log.Debugf("Create table %s: %v", tableName, fields)
	return s.db.CreateTable(tableName, fields)
}

func (s *Service) CreateColumns(tableName string, fields map[string]interface{}) error {
	s.log.Debugf("Create columns %s: %v", tableName, fields)

	s.log.Info(fields)
	tableName = utils.UniqueShortName(tableName)

	for k, v := range fields {
		val := v.(string)
		if strings.Contains(k, "UNIQUE") {
			continue

		} else if strings.Contains(k, "REFERENCES") {
			k = utils.GetFieldName(k)
			if err := s.db.AddColumn(k, tableName, k); err != nil {
				return err
			}

		} else if err := s.db.CreateColumn(tableName, k, val); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) QueryFields(tableName string, fields []string, qParams *model.QParameters) (*sql.Rows, error) {
	return s.db.Select(tableName, fields, qParams)
}

func (s *Service) TableExists(tableName string) (bool, error) {
	tableName = utils.UniqueShortName(tableName)
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

	if err := s.db.Insert(name, fields, vArray); err != nil {
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

		case reflect.TypeOf(""), reflect.TypeOf(float64(0)), reflect.TypeOf(false):
			valuesArr = append(valuesArr, v)
			fields = append(fields, field)

		default:
			fmt.Println("unknown type ", reflect.TypeOf(v))
		}
	}

	return valuesArr, fields, m, nil
}

func mapArray(m []interface{}) string {
	l := len(m)
	array := make([]string, l)
	for i := 0; i < l; i++ {
		value := m[i]
		kind := reflect.ValueOf(value).Kind()
		switch kind {
		case reflect.Map:
			array[i] = mapToString(value.(map[string]interface{}))
		default:
			array[i] = mapValueToString(value)
		}
	}
	return fmt.Sprintf("{%s}", strings.Join(array, ","))
}

func mapToString(m map[string]interface{}) (s string) {
	l := len(m)
	mapValues := make([]string, l)
	for i := 0; i < l; i++ {
		value := m[strconv.Itoa(i)]
		mapValues[i] = mapValueToString(value)
	}
	return fmt.Sprintf("{%s}", strings.Join(mapValues, ","))
}

func mapValueToString(v interface{}) string {
	kind := reflect.TypeOf(v).Kind()
	switch kind {
	case reflect.String:
		return v.(string)
	case reflect.Bool:
		boolean, _ := v.(bool)
		return strconv.FormatBool(boolean)
	default:
		fmt.Println("uknown value kind: ", kind)
		os.Exit(666)
		return ""
	}
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

		if err = s.db.Insert(field+"_r", []string{"id", table, f}, []any{uuid, entityID, id}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) LinkTable(id, linkID, idxName, tableName string) error {
	s.log.Debugf("Link table %s with %s", tableName, idxName)
	return s.db.LinkTable(id, linkID, idxName, tableName)
}

func (s *Service) SaveJsonAsEntity(parentID, name, msg string) error {
	var jsonMap map[string]interface{}

	err := json.Unmarshal([]byte(msg), &jsonMap)
	if err != nil {
		return fmt.Errorf("could not unmarshal msg: %w", err)
	}

	parentName := strcase.ToSnake(name)
	codeID := s.getCodeId(jsonMap["codeId"])
	if codeID == "" {
		_ = s.client.GetContractInfo(jsonMap["contract"].(string))
		return fmt.Errorf("code id cannot be empty %s %s", name, parentID)
	}
	entityName := fmt.Sprintf("%s_%s", parentName, codeID)

	if msg := jsonMap["msg"]; msg != nil {
		if err := s.processMsg(msg.(map[string]interface{}), parentID, entityName, parentName); err != nil {
			return fmt.Errorf("could not process message: %w", err)
		}
	}

	return nil
}

func (s *Service) processMsg(msg map[string]interface{}, parentID, name, parentName string) error {
	parentName += "s"
	tableExists, err := s.TableExists(name)
	if err != nil {
		return fmt.Errorf("could not verify if table %s exists, err: %w", name, err)
	}

	order, tables := s.generateTablesForEntity(msg, name)

	if !tableExists {
		s.log.Info("table not exists")
		for _, tableName := range order {
			if err := s.CreateTable(tableName, tables[tableName].(map[string]interface{})); err != nil {
				return fmt.Errorf("could not create table %s, err: %w", tableName, err)
			}
		}

		if err := s.AddColumn(name, parentName, name); err != nil {
			return fmt.Errorf("could not create index %s with %s, err: %w", name, parentName, err)
		}

	} else {
		for _, tableName := range order {
			if err := s.CreateColumns(tableName, tables[tableName].(map[string]interface{})); err != nil {
				return fmt.Errorf("could not create table columns  %s, err: %w", tableName, err)
			}
		}
	}

	entityID, err := s.SaveJson(name, msg)
	if err != nil {
		return fmt.Errorf("could not save json message, err: %w", err)
	}

	if err := s.LinkTable(parentID, entityID, name, parentName); err != nil {
		return fmt.Errorf("could not link table %s with %s, err: %w", name, parentName, err)
	}

	return nil
}

// func (s *Service) generateTablesForEntityWrapped(msg interface{}, name string) ([]string, map[string]interface{}) {
// 	order := make([]string, 0)
// 	relations := make([]string, 0)
// 	entityMap := make(map[string]interface{})
// 	rootEntity := make(map[string]interface{})
// 	name = utils.DeleteS(name)

// 	s.log.Info(msg)

// 	value := reflect.ValueOf(msg).String()

// 	switch value {
// 	case reflect.Map.String():
// 		return s.generateTablesForEntityMap(msg.(map[string]interface{}), name)
// 	case reflect.String.String():

// 	default:
// 		s.log.Debugf("uknown type: %s", value)
// 	}

// 	entityMap[name] = rootEntity
// 	return append(append(order, name), relations...), entityMap
// }

func (s *Service) generateTablesForEntity(msg map[string]interface{}, name string) ([]string, map[string]interface{}) {
	order := make([]string, 0)
	relations := make([]string, 0)
	entityMap := make(map[string]interface{})
	rootEntity := make(map[string]interface{})

	name = utils.DeleteS(name)

	s.log.Info(msg)

	for k, v := range msg {
		k = strcase.ToSnake(utils.DeleteS(k))
		entityName := strcase.ToSnake(utils.DeleteS(fmt.Sprintf("%s %s", name, k)))
		kind := reflect.ValueOf(v).Kind()

		s.log.Info("kind: ", kind)
		s.log.Info("key: ", k)
		s.log.Info("value: ", v)

		switch kind {

		case reflect.String:
			rootEntity[k] = "TEXT"

		case reflect.Float64, reflect.Int:
			rootEntity[k] = "BIGINT"

		case reflect.Bool:
			rootEntity[k] = "BOOLEAN"

		case reflect.Map:
			entityOrder, nestedEntity := s.generateTablesForEntity(v.(map[string]interface{}), entityName)
			for key, e := range nestedEntity {
				entityMap[key] = e
			}
			rootEntity[entityName] = fmt.Sprintf("UUID REFERENCES app.%s", utils.UniqueShortName(entityName))
			order = append(order, entityOrder...)

		case reflect.Array, reflect.Slice:
			isArray, arrayType := utils.IsArray(v.([]interface{}))

			if isArray {
				switch arrayType {
				case "String":
					rootEntity[k] = "TEXT[]"
				case "Boolean":
					rootEntity[k] = "BOOLEAN[]"
				default:
					fmt.Println("Uknown array type")
					os.Exit(666)
				}

				s.log.Info("should break")
				continue
			}

			value := v.([]interface{})[0]

			s.log.Info(value)

			// entityName := strcase.ToSnake(utils.DeleteS(fmt.Sprintf("%s %s", name, k)))
			entityOrder, nestedEntity := s.generateTablesForEntity(value.(map[string]interface{}), entityName)
			for k, e := range nestedEntity {
				entityMap[k] = e
			}

			relationTableName := entityName + "_r"
			entityMap[relationTableName] = relationTableFields(entityName, name)

			order = append(order, entityOrder...)
			relations = append(relations, relationTableName)

		// case reflect.TypeOf(nil).Kind():

		// 	entityMap[entityName] = map[string]interface{}{}

		// 	relationTableName := entityName + "_r"
		// 	entityMap[relationTableName] = relationTableFields(entityName, name)

		// 	order = append(order, entityName)
		// 	relations = append(relations, relationTableName)

		default:
			s.log.Debugf("Unhandled value type: %s key: %s", reflect.TypeOf(v).String(), k)
			os.Exit(666)
		}

	}

	entityMap[name] = rootEntity
	return append(append(order, name), relations...), entityMap
}

func (s *Service) getCodeId(codeId interface{}) string {
	if codeId == nil {
		return ""
	}

	switch reflect.TypeOf(codeId) {
	case reflect.TypeOf(map[string]interface{}{}):
		code := codeId.(map[string]interface{})["low"].(float64)
		return strconv.Itoa(int(code))
	default:
		s.log.Debugf("Unknown codeID type: %s", reflect.TypeOf(codeId).String())
		return ""
	}
}

func relationTableFields(entityName, name string) map[string]interface{} {
	en := utils.UniqueShortName(entityName)
	n := utils.UniqueShortName(name)
	return map[string]interface{}{
		en: "UUID NOT NULL",
		n:  "UUID NOT NULL",
		fmt.Sprintf("FOREIGN KEY (%s) REFERENCES app.%s(id)", en, en): "",
		fmt.Sprintf("FOREIGN KEY (%s) REFERENCES app.%s(id)", n, n):   "",
		fmt.Sprintf("UNIQUE (%s, %s)", en, n):                         "",
	}
}
