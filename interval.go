package partitioner

import (
	"fmt"
	"time"
)

// DailyInterval -- makes partitions in the format 'tableName_2020_05_15'
var DailyInterval Interval = intervalImpl{days: 1, tblFormat: "2006_01_02"}

// MonthlyInterval -- makes partitions in the format 'tableName_2020_05'
var MonthlyInterval Interval = intervalImpl{months: 1, tblFormat: "2006_01"}

type Interval interface {
	// go back one day/month (depending on the interval)
	Decrement(ts time.Time, times int) time.Time
	// go forward one day/month (depending on the interval)
	Increment(ts time.Time, times int) time.Time

	// sets the fields of ts that are too granular to 1 (for date fields) and the time to 00:00:00
	// - DailyInterval returns YYYY-MM-DD 00:00:00
	// - MonthlyInterval returns YYYY-MM-01 00:00:00
	Truncate(ts time.Time) time.Time

	// for any timestamp, returns the name of the partition
	GetPartitionName(ts time.Time, tableName string) string
}

type intervalImpl struct {
	// 1 for the field(s) that get .Increment()ed or .Decrement()ed
	days, months, years int

	// table name format (used in .GetPartitionName())
	tblFormat string
}

func (i intervalImpl) Decrement(ts time.Time, times int) time.Time {
	return ts.AddDate(-times*i.years, -times*i.months, -times*i.days)
}

func (i intervalImpl) Increment(ts time.Time, times int) time.Time {
	return ts.AddDate(times*i.years, times*i.months, times*i.days)
}

func (i intervalImpl) GetPartitionName(ts time.Time, tableName string) string {
	var suffix = ts.Format(i.tblFormat)
	return fmt.Sprintf("%s_%s", tableName, suffix)
}

func (i intervalImpl) Truncate(ts time.Time) time.Time {
	var y, m, d = ts.Date()
	if i.days == 0 {
		d = 1
	}
	return time.Date(y, m, d, 0, 0, 0, 0, ts.Location())

}
