package partitioner

import (
	"testing"
	"time"

	"github.com/mreithub/go-partitioner/driver"
	"github.com/mreithub/go-partitioner/driver/mockdriver"
	"github.com/stretchr/testify/assert"
)

func asMap(partitions ...string) map[string]bool {
	var rc = make(map[string]bool, len(partitions))
	for _, p := range partitions {
		rc[p] = true
	}
	return rc
}

func TestCreateAndDrop(t *testing.T) {
	// start with an empty 'database' and make sure our partitioner creates 3 partitions:
	// one for the current month and one for each the one before and after it
	var mock = mockdriver.New()
	var p = Partitioner{
		ParentTable: "t",
		Interval:    MonthlyInterval,
		Keep:        10,
	}

	//
	// initial run, should create "t_2023_01", "t_2023_02" and "t_2023_03"
	p.managePartitions(mock, time.Date(2023, 2, 6, 12, 34, 56, 0, time.UTC))
	assert.Equal(t, []driver.CreatePartitionInfo{
		{ParentTable: "t", Name: "t_2023_01", FromDate: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC)},
		{ParentTable: "t", Name: "t_2023_02", FromDate: time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC)},
		{ParentTable: "t", Name: "t_2023_03", FromDate: time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC)}},
		mock.Created) // one month back, one month into the future
	assert.ElementsMatch(t, nil, mock.Dropped)

	//
	// in March, only one new partition should be created: t_2023_04
	mock.ResetCreatedAndDropped()
	p.managePartitions(mock, time.Date(2023, 3, 18, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, []driver.CreatePartitionInfo{
		{ParentTable: "t", Name: "t_2023_04", FromDate: time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)}},
		mock.Created)
	assert.ElementsMatch(t, nil, mock.Dropped)

	//
	// In July, three new partitions should be created (leaving a hole where "t_2023_05" should be)
	mock.ResetCreatedAndDropped()
	p.managePartitions(mock, time.Date(2023, 7, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, []driver.CreatePartitionInfo{
		{ParentTable: "t", Name: "t_2023_06", FromDate: time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2023, 7, 1, 0, 0, 0, 0, time.UTC)},
		{ParentTable: "t", Name: "t_2023_07", FromDate: time.Date(2023, 7, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2023, 8, 1, 0, 0, 0, 0, time.UTC)},
		{ParentTable: "t", Name: "t_2023_08", FromDate: time.Date(2023, 8, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC)}},
		mock.Created)
	assert.ElementsMatch(t, nil, mock.Dropped)

	//
	// next January, it should keep everything from 2023-03 onwards (i.e. drop t_2023_01+t_2023_02) and create t_2023_12..t_2024_02
	mock.ResetCreatedAndDropped()
	p.managePartitions(mock, time.Date(2024, 1, 24, 18, 06, 0, 0, time.UTC))

	assert.Equal(t, []driver.CreatePartitionInfo{
		{ParentTable: "t", Name: "t_2023_12", FromDate: time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{ParentTable: "t", Name: "t_2024_01", FromDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)},
		{ParentTable: "t", Name: "t_2024_02", FromDate: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)}},
		mock.Created)
	assert.ElementsMatch(t, []string{"t_2023_01", "t_2023_02"}, mock.Dropped)

	//
	// check that the expected partitions exist in our mock table
	assert.Equal(t, asMap(
		"t_2023_03", "t_2023_04", // but not "t_2023_05"
		"t_2023_06", "t_2023_07", "t_2023_08",
		"t_2023_12", "t_2024_01", "t_2024_02"), mock.ExistingPartitions)

	//
	// and just to be sure, run the whole thing one last time 2 years later
	// (should drop all existing partitions and create 3 new ones)
	mock.ResetCreatedAndDropped()
	p.managePartitions(mock, time.Date(2026, 1, 24, 13, 17, 1, 0, time.UTC))
	assert.Equal(t, []driver.CreatePartitionInfo{
		{ParentTable: "t", Name: "t_2025_12", FromDate: time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{ParentTable: "t", Name: "t_2026_01", FromDate: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)},
		{ParentTable: "t", Name: "t_2026_02", FromDate: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), ToDate: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)}},
		mock.Created)

	assert.ElementsMatch(t, []string{"t_2023_03", "t_2023_04", "t_2023_06", "t_2023_07", "t_2023_08", "t_2023_12", "t_2024_01", "t_2024_02"}, mock.Dropped)
	assert.Equal(t, asMap("t_2025_12", "t_2026_01", "t_2026_02"), mock.ExistingPartitions)
}
