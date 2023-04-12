package pgxdriver

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mreithub/go-partitioner/driver"
)

type PGXDriver struct {
	DB      *pgxpool.Pool
	Context context.Context
}

func (d *PGXDriver) CreatePartition(info driver.CreatePartitionInfo) error {
	// pgx (or postgres itself) doesn't seem to support parameters for this query -> we're using unescaped time strings here
	// TODO find a way to properly sanitize input (this method should only be called internally with safe parameters but still...)
	var sql = fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
		pgx.Identifier{info.Name}.Sanitize(), pgx.Identifier{info.ParentTable}.Sanitize(), info.FromDate.Format(time.RFC3339), info.ToDate.Format(time.RFC3339))
	var _, err = d.DB.Query(d.Context, sql)
	return err
}

func (d *PGXDriver) DropPartition(name string) error {
	var _, err = d.DB.Exec(d.Context, fmt.Sprintf("DROP TABLE %s", pgx.Identifier{name}.Sanitize()))
	return err
}

func (d *PGXDriver) ListExistingPartitions(table string) (map[string]bool, error) {
	// fetch existing partitions:
	var rows, err = d.DB.Query(d.Context, "SELECT inhrelid::regclass FROM pg_catalog.pg_inherits WHERE inhparent = $1::regclass", table)
	defer rows.Close()
	if err != nil {
		return nil, err
	}

	var rc = make(map[string]bool)
	for rows.Next() {
		var partitionName string
		if err := rows.Scan(&partitionName); err != nil {
			return nil, err
		}
		rc[partitionName] = true
	}
	return rc, nil
}

var _ driver.Driver = (*PGXDriver)(nil)
