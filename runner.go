package partitioner

import (
	"context"
	"sync"
	"time"

	"github.com/mreithub/go-partitioner/driver"
)

type Runner struct {
	instances    map[*Partitioner]struct{}
	instanceLock sync.Mutex

	ctx      context.Context
	cancelFn context.CancelFunc

	ready   context.Context
	readyFn context.CancelFunc

	OnError func(m *Runner, err error) error
}

// check if our internal context has expired
func (m *Runner) Done() <-chan struct{} { return m.ctx.Done() }

// check if the initial partitioner run has finished (i.e. if all partitions we need have been created)
func (m *Runner) Ready() <-chan struct{} { return m.ready.Done() }

func (m *Runner) Add(p Partitioner) *Partitioner {
	m.instanceLock.Lock()
	defer m.instanceLock.Unlock()

	m.instances[&p] = struct{}{}

	return &p
}

func (m *Runner) filterError(err error) error {
	var onError = m.OnError
	if onError != nil {
		return onError(m, err)
	}
	return err
}

func (m *Runner) listInstances() []*Partitioner {
	m.instanceLock.Lock()
	defer m.instanceLock.Unlock()
	var rc = make([]*Partitioner, 0, len(m.instances))
	for p := range m.instances {
		rc = append(rc, p)
	}
	return rc
}

func (m *Runner) Remove(p *Partitioner) {
	m.instanceLock.Lock()
	defer m.instanceLock.Unlock()

	delete(m.instances, p)
}

func (m *Runner) run(drv driver.Driver) {
	defer m.cancelFn()

	// run once right away
	var err = m.filterError(m.runOnce(drv))
	if err != nil {
		return // abort (without the .Ready() channel being closed)
	}
	// mark this runner as ready
	m.readyFn()

	var ticker = time.NewTicker(6 * time.Hour)
	defer ticker.Stop()

	var done = false
	for !done {
		select {
		case <-ticker.C:
			m.runOnce(drv)
		case <-m.ctx.Done():
			done = true
		}
	}

	// cleanup

}

func (m *Runner) runOnce(drv driver.Driver) error {
	var now = time.Now()
	for _, p := range m.listInstances() {
		var _, err = p.ManagePartitions(drv, now)
		if err != nil {
			return err
		}
	}
	return nil
}

// Start the partition runner (wait for it to be ready afterwards)
func (m *Runner) Start(drv driver.Driver) {
	go m.run(drv)
}

func NewRunner(ctx context.Context) *Runner {
	var rc Runner
	rc.ctx, rc.cancelFn = context.WithCancel(ctx)
	rc.ready, rc.readyFn = context.WithCancel(rc.ctx)
	return &rc
}
