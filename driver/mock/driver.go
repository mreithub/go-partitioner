package mockdriver

import (
	"fmt"

	"github.com/mreithub/go-partitioner/driver"
)

type MockDriver struct {
	ExistingPartitions map[string]bool

	Created []driver.CreatePartitionInfo
	Dropped []string
}

func (d *MockDriver) CreatePartition(info driver.CreatePartitionInfo) error {
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

func (d *MockDriver) ListExistingPartitions(name string) (map[string]bool, error) {
	return d.ExistingPartitions, nil
}

var _ driver.Driver = (*MockDriver)(nil)
