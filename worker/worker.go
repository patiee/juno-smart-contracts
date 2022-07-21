package worker

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"juno-contracts-worker/indexer"
	"juno-contracts-worker/sync"

	"github.com/iancoleman/strcase"
)

type Worker struct {
	indexer *indexer.Service
	sync    *sync.Service
}

func New(indexer *indexer.Service, sync *sync.Service) *Worker {
	return &Worker{indexer: indexer, sync: sync}
}

func (w *Worker) Start(name string, height int32) error {
	var id, txHash, msg string
	var index, h int32 = 0, height
	fields := []string{"id", "index", "tx_hash", "msg"}

	for {
		rows, err := w.db.Query(strcase.ToSnake(name), fields, &h, nil)
		if err != nil {
			return err
		}

		fmt.Println("Processing height: ", h)

		for rows.Next() {
			if err = rows.Scan(&id, &index, &txHash, &msg); err != nil {
				return err
			}

			if err = w.saveEntity(id, name[0:len(name)-1], msg); err != nil {
				return err
			}

		}

		if err = w.db.Upda(h); err != nil {
			return err
		}
		h++
	}
}

func (w *Worker) saveEntity(parentID, name, msg string) error {
	var jsonMap map[string]interface{}

	err := json.Unmarshal([]byte(msg), &jsonMap)
	if err != nil {
		return err
	}

	parentName := strcase.ToCamel(name)
	entityName := parentName + getCodeId(jsonMap["codeId"])

	if msg := jsonMap["msg"]; msg != nil {
		return w.processMsg(msg.(map[string]interface{}), parentID, entityName, parentName)
	}

	return nil
}

func (w *Worker) processMsg(msg map[string]interface{}, parentID, name, parentName string) error {
	entitySchema := w.schema[name]
	if entitySchema == nil {
		lowerCamelName := strcase.ToLowerCamel(name)
		entitySchema = w.generateEntitySchema(msg, name)
		w.schema[name] = entitySchema
		w.schema[parentName].(map[string]interface{})[lowerCamelName] = name
		w.schema["Query"].(map[string]interface{})[lowerCamelName] = fmt.Sprintf("[%s]", name)

		if err := w.db.CreateTable(entitySchema.(map[string]interface{}), strcase.ToSnake(name)); err != nil {
			fmt.Println("could not create entity, ", err)
			return err
		}

		if err := w.db.CreateIndex(strcase.ToSnake(name), strcase.ToSnake(parentName+"s"), strcase.ToSnake(name)); err != nil {
			return err
		}

	}

	entityID, err := w.db.InsertJsonIntoTable(msg, strcase.ToSnake(name))
	if err != nil {
		fmt.Println("Could not insert: ", err)
		return err
	}

	if err := w.db.LinkTable(parentID, entityID, strcase.ToSnake(name), strcase.ToSnake(parentName+"s")); err != nil {
		fmt.Println("Could not link: ", err)
		return err
	}

	return nil
}

func (w *Worker) generateEntitySchema(msg map[string]interface{}, name string) map[string]interface{} {
	name = deleteS(name)

	entitySchema := make(map[string]interface{})
	for k, v := range msg {
		switch reflect.TypeOf(v) {

		case reflect.TypeOf(""):
			entitySchema[strcase.ToLowerCamel(k)] = "String"

		case reflect.TypeOf(map[string]interface{}{}):
			entityName := strcase.ToLowerCamel(deleteS(fmt.Sprintf("%s %s", name, k)))
			entityNameCamel := strcase.ToCamel(entityName)
			nestedEntitySchema := w.generateEntitySchema(v.(map[string]interface{}), entityName)
			entitySchema[entityName] = nestedEntitySchema
			w.schema[entityNameCamel] = nestedEntitySchema
			w.schema["Query"].(map[string]interface{})[entityName] = fmt.Sprintf("[%s]", entityNameCamel)

		case reflect.TypeOf([]interface{}{}):
			entityName := strcase.ToLowerCamel(deleteS(fmt.Sprintf("%s %s", name, k)))
			entityNameCamel := strcase.ToCamel(entityName)
			entity := v.([]interface{})[0].(map[string]interface{})
			nestedEntitySchema := w.generateEntitySchema(entity, entityName)
			entitySchema[entityName] = fmt.Sprintf("[%s]", entityNameCamel)
			fmt.Println("is it good key?: ", entityNameCamel)
			w.schema[entityNameCamel] = nestedEntitySchema
			w.schema["Query"].(map[string]interface{})[entityName] = fmt.Sprintf("[%s]", entityNameCamel)

		case reflect.TypeOf(float64(0)):
			entitySchema[strcase.ToLowerCamel(k)] = "Int"

		default:
			fmt.Println("k: ", k, " v: ", v)
			fmt.Println("unhandled type; ", reflect.TypeOf(v))
		}

	}
	entitySchema["id"] = "ID!"
	return entitySchema
}

func deleteS(str string) string {
	lastCh := len(str) - 1
	if string(str[lastCh]) == "s" {
		str = str[0:lastCh]
	}
	return str
}

func getCodeId(codeId interface{}) string {
	if codeId == nil {
		return ""
	}

	switch reflect.TypeOf(codeId) {
	case reflect.TypeOf(map[string]interface{}{}):
		code := codeId.(map[string]interface{})["low"].(float64)
		return strconv.Itoa(int(code))
	default:
		fmt.Println("codeID type; ", reflect.TypeOf(codeId))
		return ""
	}
}
