package worker

import (
	"juno-contracts-worker/client"
	"juno-contracts-worker/indexer"
	"juno-contracts-worker/sync"
	"juno-contracts-worker/utils"

	"github.com/iancoleman/strcase"
	"github.com/sirupsen/logrus"

	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

type Worker struct {
	client  *client.Client
	indexer *indexer.Service
	log     *logrus.Logger
	sync    *sync.Service
}

func New(c *client.Client, i *indexer.Service, l *logrus.Logger, s *sync.Service) *Worker {
	return &Worker{client: c, indexer: i, log: l, sync: s}
}

func (w *Worker) Start(name string, height int32) error {
	var id, txHash, msg string
	var index, h int32 = 0, height
	fields := []string{"id", "index", "tx_hash", "msg"}

	for {
		rows, err := w.indexer.QueryFields(strcase.ToSnake(name), fields, &h, nil)
		if err != nil {
			return fmt.Errorf("could not query message, height: %d err: %w", h, err)
		}

		idx := 0
		for rows.Next() {
			fmt.Printf("Processing height: %d msg: %d\n", h, idx)

			if err = rows.Scan(&id, &index, &txHash, &msg); err != nil {
				return fmt.Errorf("could not read fields, height: %d message index: %d err: %w", h, idx, err)
			}

			if err = w.saveEntity(id, name[0:len(name)-1], msg); err != nil {
				return fmt.Errorf("could not save entity, height: %d message index: %d err: %w", h, idx, err)
			}

			idx++
		}

		if err = w.sync.UpdateSyncHeight(h); err != nil {
			return fmt.Errorf("could not update sync height: %d err: %w", h, err)
		}
		h++
	}
}

func (w *Worker) saveEntity(parentID, name, msg string) error {
	var jsonMap map[string]interface{}

	err := json.Unmarshal([]byte(msg), &jsonMap)
	if err != nil {
		return fmt.Errorf("could not unmarshal msg: %w", err)
	}

	parentName := strcase.ToSnake(name)
	codeID := w.getCodeId(jsonMap["codeId"])
	if codeID == "" {
		_ = w.client.GetContractInfo(jsonMap["contract"].(string))
		return fmt.Errorf("code id cannot be empty %s %s", name, parentID)
	}
	entityName := fmt.Sprintf("%s_%s", parentName, codeID)

	if msg := jsonMap["msg"]; msg != nil {
		if err := w.processMsg(msg.(map[string]interface{}), parentID, entityName, parentName); err != nil {
			return fmt.Errorf("could not process message: %w", err)
		}
	}

	return nil
}

func (w *Worker) processMsg(msg map[string]interface{}, parentID, name, parentName string) error {
	parentName += "s"
	tableExists, err := w.indexer.TableExists(name)
	if err != nil {
		return fmt.Errorf("could not verify if table %s exists, err: %w", name, err)
	}

	order, tables := w.GenerateTablesForEntity(msg, name)

	if !tableExists {
		for _, tableName := range order {
			if err := w.indexer.CreateTable(tableName, tables[tableName].(map[string]interface{})); err != nil {
				return fmt.Errorf("could not create table %s, err: %w", tableName, err)
			}
		}

		if err := w.indexer.CreateIndex(name, parentName, name); err != nil {
			return fmt.Errorf("could not create index %s with %s, err: %w", name, parentName, err)
		}

	} else {
		for _, tableName := range order {
			if err := w.indexer.CreateColumns(tableName, tables[tableName].(map[string]interface{})); err != nil {
				return fmt.Errorf("could not create table columns  %s, err: %w", tableName, err)
			}
		}
	}

	entityID, err := w.indexer.SaveJson(name, msg)
	if err != nil {
		return fmt.Errorf("could not save json message, err: %w", err)
	}

	if err := w.indexer.LinkTable(parentID, entityID, name, parentName); err != nil {
		return fmt.Errorf("could not link table %s with %s, err: %w", name, parentName, err)
	}

	return nil
}

func (w *Worker) GenerateTablesForEntity(msg map[string]interface{}, name string) ([]string, map[string]interface{}) {
	name = utils.DeleteS(name)

	order := make([]string, 0)
	relations := make([]string, 0)
	entityMap := make(map[string]interface{})
	rootEntity := make(map[string]interface{})
	for k, v := range msg {
		k = strcase.ToSnake(utils.DeleteS(k))

		switch reflect.TypeOf(v) {

		case reflect.TypeOf(""):
			rootEntity[k] = "TEXT"

		case reflect.TypeOf(map[string]interface{}{}):
			entityName := strcase.ToSnake(utils.DeleteS(fmt.Sprintf("%s %s", name, k)))
			entityOrder, nestedEntity := w.GenerateTablesForEntity(v.(map[string]interface{}), entityName)
			for key, e := range nestedEntity {
				entityMap[key] = e
			}
			rootEntity[entityName] = fmt.Sprintf("UUID REFERENCES app.%s", utils.UniqueShortName(entityName))
			order = append(order, entityOrder...)

		case reflect.TypeOf([]interface{}{}):

			if isArray, arrayType := utils.IsArray(v.([]interface{})); isArray {
				switch arrayType {
				case "String":
					rootEntity[k] = "TEXT[]"
				case "Boolean":
					rootEntity[k] = "BOOLEAN[]"
				default:
					fmt.Println("Uknown array type")
				}

				continue
			}

			value := v.([]interface{})[0]

			entityName := strcase.ToSnake(utils.DeleteS(fmt.Sprintf("%s %s", name, k)))
			entityOrder, nestedEntity := w.GenerateTablesForEntity(value.(map[string]interface{}), entityName)
			for k, e := range nestedEntity {
				entityMap[k] = e
			}

			relationTableName := entityName + "_r"
			en := utils.UniqueShortName(entityName)
			n := utils.UniqueShortName(name)
			entityMap[relationTableName] = map[string]interface{}{
				en: "UUID NOT NULL",
				n:  "UUID NOT NULL",
				fmt.Sprintf("FOREIGN KEY (%s) REFERENCES app.%s(id)", en, en): "",
				fmt.Sprintf("FOREIGN KEY (%s) REFERENCES app.%s(id)", n, n):   "",
				fmt.Sprintf("UNIQUE (%s, %s)", en, n):                         "",
			}

			order = append(order, entityOrder...)
			relations = append(relations, relationTableName)

		case reflect.TypeOf(float64(0)), reflect.TypeOf(int(0)):
			rootEntity[k] = "BIGINT"

		default:
			w.log.Debugf("Unhandled value type: %s key: %s", reflect.TypeOf(v).String(), k)
		}

	}

	entityMap[name] = rootEntity
	return append(append(order, name), relations...), entityMap
}

func (w *Worker) getCodeId(codeId interface{}) string {
	if codeId == nil {
		return ""
	}

	switch reflect.TypeOf(codeId) {
	case reflect.TypeOf(map[string]interface{}{}):
		code := codeId.(map[string]interface{})["low"].(float64)
		return strconv.Itoa(int(code))
	default:
		w.log.Debugf("Unknown codeID type: %s", reflect.TypeOf(codeId).String())
		return ""
	}
}
