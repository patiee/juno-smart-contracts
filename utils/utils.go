package utils

func DeleteS(str string) string {
	lastCh := len(str) - 1
	if string(str[lastCh]) == "s" {
		str = str[0:lastCh]
	}
	return str
}
