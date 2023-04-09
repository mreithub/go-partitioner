package partitioner

import (
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mreithub/go-faster/faster"
)

// ManagePartitions -- creates new and drops old partitions for all registered tables
//
// should be run periodically
func ManagePartitions(db *pgxpool.Pool) {
	defer faster.TrackFn().Done()

	var instances []*Partitioner
	var now = time.Now()

	partitionerInstanceLock.Lock()
	instances = append(instances, partitionerInstances...)
	partitionerInstanceLock.Unlock()

	for _, instance := range instances {
		instance.managePartitions(db, now)
	}
}

// NewPartitioner -- creates and registers a partitioner instance (for the given parent table)
// - parentTable is the name of the table to partition
// - partitionType specifies whether to use daily or monthly partitions
// - keep is the number of old partitions to keep before dropping them
func NewPartitioner(parentTable string, interval Interval, keep int) *Partitioner {
	defer faster.TrackFn().Done()
	var rc = &Partitioner{
		ParentTable: parentTable,
		Interval:    interval,
		Keep:        keep,
	}

	partitionerInstanceLock.Lock()
	partitionerInstances = append(partitionerInstances, rc)
	partitionerInstanceLock.Unlock()

	return rc
}

var partitionerInstances []*Partitioner
var partitionerInstanceLock sync.Mutex
