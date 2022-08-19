package model

import (
	"fmt"
	"juno-contracts-worker/utils"
	"strings"
)

type Fields map[string]interface{}

func (f *Fields) CreateTableString() string {
	if len(*f) == 0 {
		return ""
	}

	s := ",\n"

	for k, v := range *f {
		str := v.(string)
		if strings.Contains(str, "REFERENCE") {
			k = utils.UniqueShortName(k)
		}
		s += fmt.Sprintf("%s %s,\n", k, v)
	}
	return s[0 : len(s)-2]
}

type QParameters struct {
	Limit      *int32
	StartBlock *int32
	EndBlock   *int32
	Fields     *map[string]string
	OrderBy    *map[string]string
}

func (q *QParameters) Print() (s string) {
	if (q.StartBlock != nil && *q.StartBlock != 0) || (q.EndBlock != nil && *q.EndBlock != 0) ||
		(q.Fields != nil && *q.Fields != nil) {
		whereStr := []string{}

		if q.Fields != nil {
			for k, v := range *q.Fields {
				whereStr = append(whereStr, fmt.Sprintf("%s = %s", k, v))
			}
		}

		if q.StartBlock != nil && *q.StartBlock != 0 {
			whereStr = append(whereStr, fmt.Sprintf("height >= %d", *q.StartBlock))
		}

		if q.EndBlock != nil && *q.EndBlock != 0 {
			whereStr = append(whereStr, fmt.Sprintf("height <= %d", *q.EndBlock))
		}

		s += fmt.Sprintf("WHERE %s", strings.Join(whereStr, " AND "))

	}

	if q.OrderBy != nil {
		orderByStr := []string{}
		for k, v := range *q.OrderBy {
			orderByStr = append(orderByStr, fmt.Sprintf("%s %s", k, v))
		}
		s += fmt.Sprintf(" ORDER BY %s", strings.Join(orderByStr, ", "))
	}

	if q.Limit != nil && *q.Limit != 0 {
		s += fmt.Sprintf(" LIMIT %d ", *q.Limit)
	}

	return
}

type Unsync struct {
	ID     string
	Hash   string
	Height int32
	Index  int32
	TxHash string
}
