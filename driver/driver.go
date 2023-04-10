package driver

import "time"

type CreatePartitionInfo struct {
	ParentTable      string
	Name             string
	FromDate, ToDate time.Time
}

type Driver interface {
	// returns a list of partitions for the given table
	ListExistingPartitions(table string) (map[string]bool, error)

	CreatePartition(info CreatePartitionInfo) error
	DropPartition(name string) error
}
