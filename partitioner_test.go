package internal

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestDailyPartitions(t *testing.T) {
	var queries []string
	var p = NewPartitioner("testTable", MonthlyInterval, 10)
	p.queryCb = func(sql string, params ...interface{}) bool {
		logrus.Info("query: ", sql, params)
		queries = append(queries, sql)
		return false
	}

	p.managePartitions(nil, time.Date(2016, 2, 30, 12, 34, 56, 0, time.Local))
}
