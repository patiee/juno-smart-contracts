package schema

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/iancoleman/strcase"
)

func resolver(fields map[string]interface{}, name string) []byte {
	return []byte(fmt.Sprintf(`package schema

%s

%s %s
`, printImports(fields), printResolver(fields, name), printFields(fields, name)))
}

func printImports(fields map[string]interface{}) string {
	importStr := `import (
	"fmt"

	"github.com/iancoleman/strcase"
	"github.com/graph-gophers/graphql-go"
	
`
	// for _, v := range fields {
	// 	if strings.TrimSpace(v.(string)) == "BigFloat" {
	// 		// importStr += "\t\"math/big\"\n"
	// 		importStr += "\t\"encoding/json\"\n"
	// 	}
	// }
	importStr += ")"
	return importStr
}

func printResolver(fields map[string]interface{}, name string) string {
	fieldsStr := ""
	for k, v := range fields {
		switch reflect.TypeOf(v) {
		case reflect.TypeOf(""):
			fieldsStr += printResolverField(k, v.(string))
		default:
			fieldsStr += printResolverField(k, k)
		}

	}
	return fmt.Sprintf(`type %sResolver struct {
%s}

`, name, fieldsStr)
}

func printResolverField(fieldName, fieldType string) string {
	return fmt.Sprintf("\t%s\t%s\n", fieldName, printFieldType(fieldType))
}

func printFieldType(fieldType string) string {
	fieldType = strings.TrimSpace(fieldType)
	switch fieldType {
	case "ID!":
		return "graphql.ID"
	case "String":
		return "*string"
	case "String!":
		return "string"
	case "Int":
		return "*int32"
	case "BigFloat":
		return "*int64"
	default:
		if string(fieldType[0]) == "[" {
			fieldType = fieldType[1 : len(fieldType)-1]
			return fmt.Sprintf("*[]*%sResolver", strcase.ToLowerCamel(fieldType))
		} else {
			return fmt.Sprintf("*%sResolver", strcase.ToLowerCamel(fieldType))
		}
	}
}

func isResolver(value interface{}) bool {
	if reflect.TypeOf(value) == reflect.TypeOf(map[string]interface{}{}) {
		return true
	}

	str := strings.TrimSpace(value.(string))
	switch str {
	case "ID!", "String", "String!", "Int", "BigFloat":
		return false
	default:
		return true
	}
}

func printFields(fields map[string]interface{}, name string) string {
	name = strcase.ToLowerCamel(name)
	upperCaseName := strcase.ToCamel(name)
	ids, idsMapping, fieldString, rowString := buildFieldsQueryRowAndIds(fields)
	str := fmt.Sprintf(`
var %sFields = []string{%s}

func (q *Query) %s(args *Parameters) *[]*%sResolver {
	result := []*%sResolver{}
	var height int32
	var id string
	if args != nil {
		if args.Height != nil {
			height = *args.Height
		}
		if args.ID != nil {
			id = *args.ID
		}
	}
	rows, err := q.db.Query(strcase.ToSnake("%s"), %sFields, &height, &id)
	if err != nil {
		fmt.Println("Error while querying: ", err)
		return nil
	}
	for rows.Next() {
		msg := new(%sResolver)
		%s
		err = rows.Scan(%s)
		if err != nil {
			fmt.Println("Error while scanning %s: ", err)
			return nil
		}
%s
		result = append(result, msg)
	}
	return &result
}

`, name, fieldString, upperCaseName, name, name, name, name, name, ids, rowString, name, idsMapping)
	for k, v := range fields {
		str += printField(v, k, name)
	}
	return str
}

func buildFieldsQueryRowAndIds(fields map[string]interface{}) (idsString string, idsMappingString string, fieldStr string, row string) {
	for k, v := range fields {
		snake := strcase.ToSnake(k)
		if snake != "id" {
			snake = strings.Replace(snake, "id", "i_d", -1)
		}
		fieldStr += fmt.Sprintf("\"%s\", ", snake)

		if isResolver(v) {
			idsString += fmt.Sprintf("\t\tvar %s string\n", k)
			funcName := strcase.ToCamel(k)
			fLen := len(funcName)
			if string(funcName[fLen-1]) == "s" {
				funcName = funcName[0 : fLen-1]
			}

			if reflect.TypeOf(v) == reflect.TypeOf("") && string(v.(string)[0]) == "[" {
				idsMappingString += fmt.Sprintf("\t\tmsg.%s = q.%s(&Parameters{ID: &%s})\n", k, funcName, k)
			} else {
				idsMappingString += fmt.Sprintf("\t\tmsg.%s = (*q.%s(&Parameters{ID: &%s}))[0]\n", k, funcName, k)
			}

			row += fmt.Sprintf("&%s, ", k)
		} else {
			row += fmt.Sprintf("&msg.%s, ", k)
		}
	}
	fieldStr = fieldStr[0 : len(fieldStr)-2]
	return idsString, idsMappingString, fieldStr, row[0 : len(row)-2]
}

func printField(value interface{}, fieldName, resolverName string) string {
	fieldValue := fieldName

	if reflect.TypeOf(value) != reflect.TypeOf(map[string]interface{}{}) {
		fieldValue = strings.TrimSpace(value.(string))
	}

	returnStr := fmt.Sprintf("return r.%s", fieldName)
	return fmt.Sprintf(`
func (r *%sResolver) %s() %s {
	%s
}
`, resolverName, strcase.ToCamel(fieldName), printFieldType(fieldValue), returnStr)
}

func GenerateResolvers(schema map[string]interface{}, path string) error {
	for key, value := range schema["Query"].(map[string]interface{}) {
		str := strings.TrimSpace(value.(string))
		isArray := strings.Contains(str, "[")
		if isArray {
			str = str[1 : len(str)-1]
		}

		if err := GenerateResolver(schema[str].(map[string]interface{}), path, key); err != nil {
			return err
		}
	}
	return nil
}

func GenerateResolver(fields map[string]interface{}, path, name string) error {
	newFilePath := path + name + ".go"
	resolverString := resolver(fields, name)

	if err := os.WriteFile(newFilePath, resolverString, os.ModePerm); err != nil {
		return err
	}

	return nil
}
