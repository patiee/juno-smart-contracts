package mapping

import (
	"fmt"
	"reflect"

	"github.com/iancoleman/strcase"
)

func ParseMapToSchema(schema map[string]interface{}) string {
	return fmt.Sprintf(`schema {
	query: Query
}
	
%s
`, printTypes(schemaWithQueryParameters(schema)))
}

func schemaWithQueryParameters(schema map[string]interface{}) map[string]interface{} {
	newSchema := make(map[string]interface{})
	for k, v := range schema {
		if k == "Query" {
			newQuery := make(map[string]interface{})
			for name, value := range v.(map[string]interface{}) {
				newQuery[name+"(id: String, height: Int)"] = value
			}
			newSchema["Query"] = newQuery
		} else {
			newSchema[k] = v
		}
	}
	return newSchema
}

func printTypes(schema map[string]interface{}) string {
	str := ""
	for name, fields := range schema {
		str += printType(fields.(map[string]interface{}), name)
	}
	return str
}

func printType(fields map[string]interface{}, name string) string {
	return fmt.Sprintf(`type %s {
%s}

`, name, printFields(fields))
}

func printFields(fields map[string]interface{}) string {
	str := ""
	for k, v := range fields {
		switch reflect.TypeOf(v) {
		case reflect.TypeOf(map[string]interface{}{}):
			str += printField(k, strcase.ToCamel(k))
		case reflect.TypeOf([]interface{}{}):
			str += printField(k, fmt.Sprintf("[%s]", strcase.ToCamel(k)))
		case reflect.TypeOf(""):
			str += printField(k, v.(string))
		default:
			fmt.Println("Unknown field type!")
		}

	}
	return str
}

func printField(name, value string) string {
	return fmt.Sprintf("\t%s: %s\n", name, value)
}
