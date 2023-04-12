package mockdriver

import (
	"fmt"

	"github.com/mreithub/go-partitioner/driver"
	"github.com/sirupsen/logrus"
)

// interface declarations
var _ driver.Driver = (*MockDriver)(nil)

// fake database connector that keeps track of created and deleted partitions (used for unit tests)
type MockDriver struct {
	ExistingPartitions map[string]bool

	Created []driver.CreatePartitionInfo
	Dropped []string
}

func (d *MockDriver) CreatePartition(info driver.CreatePartitionInfo) error {
	logrus.WithField("info", info).Info("creating partition")
	if _, ok := d.ExistingPartitions[info.Name]; ok {
		return fmt.Errorf("partition already exists: %q", info.Name)
	}
	d.Created = append(d.Created, info)
	d.ExistingPartitions[info.Name] = true
	return nil
}

func (d *MockDriver) DropPartition(name string) error {
	if _, ok := d.ExistingPartitions[name]; !ok {
		return fmt.Errorf("no such partition: %q", name)
	}
	d.Dropped = append(d.Dropped, name)
	delete(d.ExistingPartitions, name)
	return nil
}

// returns a copy of the current list of partitions
func (d *MockDriver) ListExistingPartitions(name string) (map[string]bool, error) {
	var rc = make(map[string]bool, len(d.ExistingPartitions))
	for k, v := range d.ExistingPartitions {
		rc[k] = v
	}
	return rc, nil
}

func (d *MockDriver) ResetCreatedAndDropped() {
	d.Created = nil
	d.Dropped = nil
}

func New(existingPartitions ...string) *MockDriver {
	var asDict = make(map[string]bool)
	for _, p := range existingPartitions {
		asDict[p] = true
	}

	return &MockDriver{
		ExistingPartitions: asDict,
	}
}
