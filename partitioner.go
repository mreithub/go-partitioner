package partitioner

import (
	"fmt"
	"time"

	"github.com/mreithub/go-faster/faster"
	"github.com/mreithub/go-partitioner/driver"
	"github.com/sirupsen/logrus"
)

type Interval bool

// DailyInterval -- makes partitions in the format 'tableName_2020_05_15'
const DailyInterval = Interval(true)

// MonthlyInterval -- makes partitions in the format 'tableName_2020_05'
const MonthlyInterval = Interval(false)

// Partitioner - manages postgres partitions for a table
type Partitioner struct {
	ParentTable string
	Interval    Interval
	Keep        int

	Logf func(format string, args ...interface{})
}

func (p Partitioner) decrement(ts time.Time, times int) time.Time {
	if p.Interval == DailyInterval {
		return ts.AddDate(0, 0, -times)
	}
	return ts.AddDate(0, -times, 0)
}

func (p Partitioner) increment(ts time.Time, times int) time.Time {
	if p.Interval == DailyInterval {
		return ts.AddDate(0, 0, times)
	}
	return ts.AddDate(0, times, 0)
}

func (p Partitioner) log(format string, args ...interface{}) {
	if p.Logf != nil {
		p.Logf(format, args...)
	}
}

func (p Partitioner) truncate(ts time.Time) time.Time {
	var y, m, d = ts.Date()
	if p.Interval == MonthlyInterval {
		d = 1
	}
	return time.Date(y, m, d, 0, 0, 0, 0, ts.Location())
}

func (p Partitioner) getPartitionName(ts time.Time) string {
	var suffix string
	if p.Interval == DailyInterval {
		suffix = ts.Format("2006_01_02")
	} else {
		suffix = ts.Format("2006_01")
	}
	return fmt.Sprintf("%s_%s", p.ParentTable, suffix)
}

func (p Partitioner) managePartitions(drv driver.Driver, now time.Time) error {
	defer faster.TrackFn().Done()
	now = now.UTC()

	var existingPartitions, err = drv.ListExistingPartitions(p.ParentTable)
	if err != nil {
		return fmt.Errorf("failed to list existing partitions: %w", err)
	}

	var countExisting = len(existingPartitions)

	// enumerate the ones we want to keep/create
	var minKeepTs = p.decrement(p.truncate(now), p.Keep)
	var minCreateTs = p.decrement(p.truncate(now), 1)
	var maxTs = p.increment(p.truncate(now), 1) // create one entry into the future
	var ts = minKeepTs
	var countCreated = 0
	for !ts.After(maxTs) {
		var partition = p.getPartitionName(ts)
		var fromDate = ts
		var toDate = p.increment(ts, 1)

		if !existingPartitions[partition] && !ts.Before(minCreateTs) {
			// doesn't yet exist and ts >= minCreateTs  -> create it
			err = drv.CreatePartition(driver.CreatePartitionInfo{
				Name:        partition,
				ParentTable: p.ParentTable,
				FromDate:    fromDate, ToDate: toDate})
			if err != nil {
				return fmt.Errorf("failed to create partition %q: %w", partition, err)
			}
			countCreated += 1
		} else if existingPartitions[partition] {
			// we'll set 'valid' entries in existingPartition to false (whatever remains true will be deleted in the next step)
			existingPartitions[partition] = false
		}

		ts = p.increment(ts, 1)
	}

	// drop everything we didn't loop over
	var countDropped = 0
	for partition, shouldBeDeleted := range existingPartitions {
		if shouldBeDeleted {
			if err = drv.DropPartition(partition); err != nil {
				return fmt.Errorf("failed to drop partition %q: %w", partition, err)
			}
			countDropped += 1
		}
	}
	logrus.Infof("partitioner for %q: %d existing partitions - %d created, %d dropped", p.ParentTable, countExisting, countCreated, countDropped)
	return nil
}
