package parser

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"

	"juno-contracts-worker/database"
	"juno-contracts-worker/schema/mapping"

	"github.com/iancoleman/strcase"
)

type Parser struct {
	db     *database.DB
	schema map[string]interface{}
}

func New(db *database.DB, schema map[string]interface{}) *Parser {
	return &Parser{db: db, schema: schema}
}

func (p *Parser) StartParsing(name string, height int32) error {
	var id, txHash, msg string
	var index, h int32 = 0, height
	fields := []string{"id", "index", "tx_hash", "msg"}

	for {
		rows, err := p.db.Query(strcase.ToSnake(name), fields, &h, nil)
		if err != nil {
			return err
		}

		fmt.Println("Processing height: ", h)

		for rows.Next() {
			if err = rows.Scan(&id, &index, &txHash, &msg); err != nil {
				return err
			}

			if err = p.saveEntity(id, name[0:len(name)-1], msg); err != nil {
				return err
			}

		}

		if err = p.db.UpdateStateHeight(h); err != nil {
			return err
		}
		h++
	}
}

func (p *Parser) saveEntity(parentID, name, msg string) error {
	var jsonMap map[string]interface{}

	err := json.Unmarshal([]byte(msg), &jsonMap)
	if err != nil {
		return err
	}

	parentName := strcase.ToCamel(name)
	entityName := parentName + getCodeId(jsonMap["codeId"])

	if msg := jsonMap["msg"]; msg != nil {
		return p.processMsg(msg.(map[string]interface{}), parentID, entityName, parentName)
	}

	return nil
}

func (p *Parser) processMsg(msg map[string]interface{}, parentID, name, parentName string) error {
	entitySchema := p.schema[name]
	if entitySchema == nil {
		lowerCamelName := strcase.ToLowerCamel(name)
		entitySchema = p.generateEntitySchema(msg, name)
		p.schema[name] = entitySchema
		p.schema[parentName].(map[string]interface{})[lowerCamelName] = name
		p.schema["Query"].(map[string]interface{})[lowerCamelName] = fmt.Sprintf("[%s]", name)

		if err := p.db.CreateTable(entitySchema.(map[string]interface{}), strcase.ToSnake(name)); err != nil {
			fmt.Println("could not create entity, ", err)
			return err
		}

		if err := p.db.CreateIndex(strcase.ToSnake(name), strcase.ToSnake(parentName+"s"), strcase.ToSnake(name)); err != nil {
			return err
		}

		if err := p.saveNewSchema(); err != nil {
			return err
		}
	}

	entityID, err := p.db.InsertJsonIntoTable(msg, strcase.ToSnake(name))
	if err != nil {
		fmt.Println("Could not insert: ", err)
		return err
	}

	if err := p.db.LinkTable(parentID, entityID, strcase.ToSnake(name), strcase.ToSnake(parentName+"s")); err != nil {
		fmt.Println("Could not link: ", err)
		return err
	}

	return nil
}

func (p *Parser) generateEntitySchema(msg map[string]interface{}, name string) map[string]interface{} {
	name = deleteS(name)

	entitySchema := make(map[string]interface{})
	for k, v := range msg {
		switch reflect.TypeOf(v) {

		case reflect.TypeOf(""):
			entitySchema[k] = "String"

		case reflect.TypeOf(map[string]interface{}{}):
			entityName := strcase.ToLowerCamel(deleteS(fmt.Sprintf("%s %s", name, k)))
			entityNameCamel := strcase.ToCamel(entityName)
			nestedEntitySchema := p.generateEntitySchema(v.(map[string]interface{}), entityName)
			entitySchema[entityName] = nestedEntitySchema
			p.schema[entityNameCamel] = nestedEntitySchema
			p.schema["Query"].(map[string]interface{})[entityName] = fmt.Sprintf("[%s]", entityNameCamel)

		case reflect.TypeOf([]interface{}{}):
			entityName := strcase.ToLowerCamel(deleteS(fmt.Sprintf("%s %s", name, k)))
			entityNameCamel := strcase.ToCamel(entityName)
			entity := v.([]interface{})[0].(map[string]interface{})
			nestedEntitySchema := p.generateEntitySchema(entity, entityName)
			entitySchema[entityName] = fmt.Sprintf("[%s]", entityNameCamel)
			fmt.Println("is it good key?: ", entityNameCamel)
			p.schema[entityNameCamel] = nestedEntitySchema
			p.schema["Query"].(map[string]interface{})[entityName] = fmt.Sprintf("[%s]", entityNameCamel)

		case reflect.TypeOf(float64(0)):
			entitySchema[k] = "Int"

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

func (p *Parser) saveNewSchema() error {
	return os.WriteFile("schema.graphql", []byte(mapping.ParseMapToSchema(p.schema)), os.ModePerm)
}
