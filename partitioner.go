package partitioner

import (
	"fmt"
	"time"

	"github.com/mreithub/go-faster/faster"
	"github.com/mreithub/go-partitioner/driver"
)

// Partitioner - manages postgres partitions for a table
type Partitioner struct {
	ParentTable string
	Interval    Interval
	// number of partitions to keep (e.g. 6 means '6 days' or '6 months' of partitions, depending on .Interval)
	Keep int

	// if set, you can prevent certain partitions from being deleted
	// (use this for example to prevent deletion of a default partition - or to prevent automatic deletion altogether)
	CanDropFn func(partitionName string) bool
}

func (p Partitioner) ManagePartitions(drv driver.Driver, now time.Time) (RunInfo, error) {
	defer faster.TrackFn().Done()
	now = now.UTC()

	var existingPartitions, err = drv.ListExistingPartitions(p.ParentTable)
	if err != nil {
		return RunInfo{}, fmt.Errorf("failed to list existing partitions: %w", err)
	}

	var rc = RunInfo{
		Existing: len(existingPartitions),
	}

	// enumerate the ones we want to keep/create
	var minKeepTs = p.Interval.Decrement(p.Interval.Truncate(now), p.Keep)
	var minCreateTs = p.Interval.Decrement(p.Interval.Truncate(now), 1)
	var maxTs = p.Interval.Increment(p.Interval.Truncate(now), 1) // create one entry into the future
	var ts = minKeepTs
	for !ts.After(maxTs) {
		var partition = p.Interval.GetPartitionName(ts, p.ParentTable)
		var fromDate = ts
		var toDate = p.Interval.Increment(ts, 1)

		if !existingPartitions[partition] && !ts.Before(minCreateTs) {
			// doesn't yet exist and ts >= minCreateTs  -> create it
			err = drv.CreatePartition(driver.CreatePartitionInfo{
				Name:        partition,
				ParentTable: p.ParentTable,
				FromDate:    fromDate, ToDate: toDate})
			if err != nil {
				return rc, fmt.Errorf("failed to create partition %q: %w", partition, err)
			}
			rc.Created += 1
		} else if existingPartitions[partition] {
			// we'll set 'valid' entries in existingPartition to false (whatever remains true will be deleted in the next step)
			existingPartitions[partition] = false
		}

		ts = p.Interval.Increment(ts, 1)
	}

	// drop everything we didn't loop over
	for partition, shouldBeDeleted := range existingPartitions {
		if shouldBeDeleted {
			if p.CanDropFn != nil && !p.CanDropFn(partition) {
				continue
			}

			if err = drv.DropPartition(partition); err != nil {
				return rc, fmt.Errorf("failed to drop partition %q: %w", partition, err)
			}
			rc.Dropped += 1
		}
	}

	return rc, nil
}
