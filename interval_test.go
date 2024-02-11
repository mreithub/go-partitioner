package partitioner_test

import (
	"testing"
	"time"

	"github.com/mreithub/go-partitioner"
	"github.com/stretchr/testify/assert"
)

var DailyInterval = partitioner.DailyInterval
var MonthlyInterval = partitioner.MonthlyInterval
var YearlyInterval = partitioner.YearlyInterval

func TestIntervals(t *testing.T) {
	var h, m, s, ns = 12, 34, 56, 789
	var ts = time.Date(2024, 2, 29, h, m, s, ns, time.Local)

	var truncAndInc = func(i partitioner.Interval, ts time.Time, times int) time.Time {
		return i.Increment(i.Truncate(ts), times)
	}

	assert.Equal(t, "foo_2024_02_29", DailyInterval.GetPartitionName(ts, "foo"))
	assert.Equal(t, "foo_2024_03_07", DailyInterval.GetPartitionName(truncAndInc(DailyInterval, ts, 7), "foo"))

	assert.Equal(t, "foo_2024_02", MonthlyInterval.GetPartitionName(ts, "foo"))
	assert.Equal(t, "foo_2024_04", MonthlyInterval.GetPartitionName(truncAndInc(MonthlyInterval, ts, 2), "foo"))

	assert.Equal(t, "foo_2024", YearlyInterval.GetPartitionName(ts, "foo"))
	assert.Equal(t, "foo_2025", YearlyInterval.GetPartitionName(truncAndInc(YearlyInterval, ts, 1), "foo"))

	assert.Equal(t, time.Date(2024, 2, 29, 0, 0, 0, 0, time.Local), DailyInterval.Truncate(ts))
	assert.Equal(t, time.Date(2024, 2, 1, 0, 0, 0, 0, time.Local), MonthlyInterval.Truncate(ts))
	assert.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.Local), YearlyInterval.Truncate(ts))

	assert.Equal(t, time.Date(2024, 3, 1, h, m, s, ns, time.Local), DailyInterval.Increment(ts, 1))
	assert.Equal(t, time.Date(2024, 3, 29, h, m, s, ns, time.Local), MonthlyInterval.Increment(ts, 1))
	assert.Equal(t, time.Date(2025, 3, 1, h, m, s, ns, time.Local), YearlyInterval.Increment(ts, 1)) // 2025 is not a leap year -> doesn't have Feb29

	assert.Equal(t, time.Date(2024, 2, 28, h, m, s, ns, time.Local), DailyInterval.Decrement(ts, 1))
	assert.Equal(t, time.Date(2024, 1, 29, h, m, s, ns, time.Local), MonthlyInterval.Decrement(ts, 1))
	assert.Equal(t, time.Date(2023, 3, 1, h, m, s, ns, time.Local), YearlyInterval.Decrement(ts, 1)) // 2023 is not a leap year -> doesn't have Feb29

	assert.Equal(t, time.Date(2024, 3, 1, 0, 0, 0, 0, time.Local), truncAndInc(DailyInterval, ts, 1))
	assert.Equal(t, time.Date(2024, 3, 1, 0, 0, 0, 0, time.Local), truncAndInc(MonthlyInterval, ts, 1))
	assert.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local), truncAndInc(YearlyInterval, ts, 1))
}
