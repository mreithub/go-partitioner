package logrusdriver

import (
	"github.com/mreithub/go-partitioner/driver"
	"github.com/sirupsen/logrus"
)

var _ driver.Driver = (*LogrusDriver)(nil)

type LogrusDriver struct {
	Next driver.Driver
}

func (d LogrusDriver) ListExistingPartitions(table string) (map[string]bool, error) {
	return d.Next.ListExistingPartitions(table)
}

func (d LogrusDriver) CreatePartition(info driver.CreatePartitionInfo) error {
	var err = d.Next.CreatePartition(info)
	if err != nil {
		logrus.WithError(err).Errorf("failed to create partition %q", info.Name)
		return err
	}
	logrus.Infof("created partition %q", info.Name)
	return err
}

func (d LogrusDriver) DropPartition(name string) error {
	var err = d.Next.DropPartition(name)
	if err != nil {
		logrus.WithError(err).Errorf("failed to drop partition %q", name)
		return err
	}
	logrus.Infof("dropped partition %q", name)
	return nil
}
