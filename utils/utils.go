package utils

import (
	"reflect"
	"strconv"
	"strings"
)

func DeleteS(str string) string {
	lastCh := len(str) - 1
	if string(str[lastCh]) == "s" {
		str = str[0:lastCh]
	}
	return str
}

func AddUnderscoresIfMissing(strs []string) []string {
	underscores := make([]string, len(strs))
	for idx, str := range strs {
		underscores[idx] = AddUnderscoreIfMissing(str)
	}
	return underscores
}

func AddUnderscoreIfMissing(str string) string {
	if string(str[0]) == "m" && !strings.Contains(str, "_") {
		return str
	}

	strArr := strings.Split(str, "_")

	for i := 0; i < len(strArr); i++ {
		s := strArr[i]
		sLen := len(s)
		for idx := 0; idx < sLen; idx++ {
			ch := string(s[idx])
			if _, err := strconv.Atoi(ch); err == nil && idx > 1 && string(s[idx-2]) != "_" {
				strArr[i] = s[0:idx] + "_" + s[idx:sLen]
				break
			}
		}
	}

	return strings.Join(strArr, "_")
}

func UniqueShortNames(names []string) (shortNames []string) {
	shortNames = make([]string, len(names))
	for idx, name := range names {
		shortNames[idx] = UniqueShortName(name)
	}
	return shortNames
}

func UniqueShortName(name string) (shortName string) {
	arr := strings.Split(name, "_")
	l := len(arr)
	for i, s := range arr {
		_, err := strconv.Atoi(s)
		if err == nil || i > l-3 {
			shortName += s
		} else {
			shortName += string(s[0])
		}
	}

	return shortName
}

func IsArray(val []interface{}) (bool, string) {
	typeOfValue := reflect.TypeOf(val[0])
	if reflect.TypeOf(map[string]interface{}{}) == typeOfValue {
		return isMapArray(val[0].(map[string]interface{}))
	}
	return false, ""
}

func isMapArray(m map[string]interface{}) (bool, string) {
	if k, ok := m["0"]; ok && k != nil && len(m) > 0 {
		return true, reflect.TypeOf(k).String()
	}
	return false, ""
}

func GetFieldName(str string) string {
	startIdx, endIdx := 0, 0
	for i := 0; i < len(str); i++ {
		switch string(str[i]) {
		case "(":
			startIdx = i + 1
		case ")":
			endIdx = i
			return str[startIdx:endIdx]
		}
	}
	return ""
}
