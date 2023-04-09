package internal

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mreithub/go-faster/faster"
	"github.com/sirupsen/logrus"
)

type partitionType bool

// PartitionDaily -- makes partitions in the format 'tableName_2020_05_15'
const PartitionDaily = partitionType(true)

// PartitionMonthly -- makes partitions in the format 'tableName_2020_05'
const PartitionMonthly = partitionType(false)

// Partitioner -- returned by NewPartitioner() - only used internally so far
type Partitioner interface {
	managePartitions(db *pgxpool.Pool, now time.Time)
}

type partitioner struct {
	parentTable string
	isDaily     partitionType
	keep        int

	// queryCb - used in unit tests to test which queries would end up being sent to the db server (if this returns false, no query is sent to the DB servr)
	queryCb func(sql string, params ...interface{}) bool
}

func (p *partitioner) exec(db *pgxpool.Pool, sql string, params ...interface{}) (pgconn.CommandTag, error) {
	if p.queryCb == nil || p.queryCb(sql, params) {
		return db.Exec(context.Background(), sql, params...)
	}
	return pgconn.CommandTag{}, errors.New("query filtered")
}

func (p *partitioner) decrement(ts time.Time, times int) time.Time {
	if p.isDaily {
		return ts.AddDate(0, 0, -times)
	}
	return ts.AddDate(0, -times, 0)
}

func (p *partitioner) increment(ts time.Time) time.Time {
	if p.isDaily {
		return ts.AddDate(0, 0, 1)
	}
	return ts.AddDate(0, 1, 0)
}

func (p *partitioner) truncate(ts time.Time) time.Time {
	var y, m, d = ts.Date()
	if !p.isDaily {
		d = 1
	}
	return time.Date(y, m, d, 0, 0, 0, 0, ts.Location())
}

func (p *partitioner) createPartition(db *pgxpool.Pool, name string, fromDate time.Time, toDate time.Time) error {
	defer faster.TrackFn().Done()
	var err error

	// pgx (or postgres itself) doesn't seem to support parameters for this query -> we're using unescaped time strings here
	// TODO find a way to properly sanitize input (this method should only be called internally with safe parameters but still...)
	var sql = fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
		pgx.Identifier{name}.Sanitize(), pgx.Identifier{p.parentTable}.Sanitize(), fromDate.Format(time.RFC3339), toDate.Format(time.RFC3339))
	if _, err = p.exec(db, sql); err == nil {
		logrus.Info("created partition ", name)
	} else if err, ok := err.(*pgconn.PgError); ok && err.Code == pgerrcode.DuplicateTable {
		// partition already exists - ignored
	} else {
		logrus.WithError(err).Error("create partition failed: ", reflect.TypeOf(err))
		panic("hi")
	} //*/

	return err
}

func (p *partitioner) dropPartition(db *pgxpool.Pool, name string) error {
	defer faster.TrackFn().Done()
	var err error

	if _, err = p.exec(db, fmt.Sprintf("DROP TABLE %s", pgx.Identifier{name}.Sanitize())); err == nil {
		logrus.Info("dropped partition ", name)
	} else if err, ok := err.(*pgconn.PgError); ok && err.Code == pgerrcode.UndefinedTable {
		// table didn't exist - ignored
	} else {
		logrus.WithError(err).Error("dropping partition failed")
	}

	return err
}

func (p *partitioner) getPartitionName(ts time.Time) string {
	var suffix string
	if p.isDaily {
		suffix = ts.Format("2006_01_02")
	} else {
		suffix = ts.Format("2006_01")
	}
	return fmt.Sprintf("%s_%s", p.parentTable, suffix)
}

func (p *partitioner) managePartitions(db *pgxpool.Pool, now time.Time) {
	defer faster.TrackFn().Done()
	now = now.UTC()

	// create future partitions (starting one in the past)
	var ts = p.decrement(p.truncate(now), 1)
	for i := 0; i < 4; i++ {
		var partition = p.getPartitionName(ts)
		p.createPartition(db, partition, ts, p.increment(ts))
		ts = p.increment(ts)
	}

	// delete old partitions
	var limit = 30 // when in daily mode, delete up to 30 old partitions
	if !p.isDaily {
		limit = 6
	}
	ts = p.decrement(p.truncate(now), p.keep+1)
	for i := 0; i < limit; i++ {
		var partition = p.getPartitionName(ts)
		p.dropPartition(db, partition)
		ts = p.decrement(ts, 1)
	}
}

// NewPartitioner -- creates and registers a partitioner instance (for the given parent table)
// - parentTable is the name of the table to partition
// - partitionType specifies whether to use daily or monthly partitions
// - keep is the number of old partitions to keep before dropping them
func NewPartitioner(parentTable string, partitionType partitionType, keep int) Partitioner {
	defer faster.TrackFn().Done()
	var rc = &partitioner{
		parentTable: parentTable,
		isDaily:     partitionType,
		keep:        keep,
	}

	partitionerInstanceLock.Lock()
	partitionerInstances = append(partitionerInstances, rc)
	partitionerInstanceLock.Unlock()

	return rc
}

// ManagePartitions -- creates new and drops old partitions for all registered tables
//
// should be run periodically
func ManagePartitions(db *pgxpool.Pool) {
	defer faster.TrackFn().Done()

	var instances []Partitioner
	var now = time.Now()

	partitionerInstanceLock.Lock()
	instances = append(instances, partitionerInstances...)
	partitionerInstanceLock.Unlock()

	for _, instance := range instances {
		instance.managePartitions(db, now)
	}
}

var partitionerInstances []Partitioner
var partitionerInstanceLock sync.Mutex
