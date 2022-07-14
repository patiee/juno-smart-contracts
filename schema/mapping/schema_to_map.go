package mapping

import (
	"regexp"
	"strings"
)

var schemaType = regexp.MustCompile(`type [^}]*}`)

// var enumType = regexp.MustCompile(`enum [^}]*}`)

func ParseSchemaToMap(schema string) map[string]interface{} {
	schemaMap := make(map[string]interface{})
	results := schemaType.FindAll([]byte(schema), -1)
	for _, result := range results {
		strResult := string(result)
		lines := strings.Split(strResult, "\n")

		if len(lines) < 1 {
			continue
		}

		typeName := lines[0]
		typeName = typeName[5 : len(typeName)-2]

		fieldsMap := make(map[string]interface{})
		for i := 1; i < len(lines)-1; i++ {
			line := lines[i]
			field := strings.Split(strings.TrimSpace(line), ":")

			key := removeParameters(field[0])
			value := field[1]
			if len(field) > 3 {
				value = field[3]
			}

			fieldsMap[key] = value
		}

		schemaMap[typeName] = fieldsMap

	}
	// TODO enums
	return schemaMap
}

func removeParameters(str string) string {
	str = strings.TrimSpace(str)
	for i, ch := range str {
		if string(ch) == "(" {
			return str[0:i]
		}
	}
	return str
}
