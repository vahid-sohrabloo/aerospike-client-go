package aerospike

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// fastExec runs the nodes of a FastBatch that the caller does not run itself.
// A submit never queues: it hands the node to an idle worker, starts a new
// worker below the cap, or else falls back to a plain goroutine.
type fastExec struct {
	once    sync.Once
	stopped sync.Once
	work    chan *fastBatchCommand
	done    chan struct{}
	workers atomic.Int32
	max     int32
	spilled atomic.Int64
}

func (clnt *Client) fastPool() *fastExec {
	p := &clnt.fast
	p.once.Do(func() {
		p.work = make(chan *fastBatchCommand)
		p.done = make(chan struct{})
		p.max = int32(4 * runtime.GOMAXPROCS(0))
	})
	return p
}

func (clnt *Client) stopFastPool() {
	p := clnt.fastPool()
	p.stopped.Do(func() { close(p.done) })
}

func (p *fastExec) submit(cmd *fastBatchCommand) {
	select {
	case p.work <- cmd:
		return
	default:
	}
	if p.workers.Add(1) <= p.max {
		go p.worker(cmd)
		return
	}
	p.workers.Add(-1)
	p.spilled.Add(1)
	go runNode(cmd)
}

func (p *fastExec) worker(cmd *fastBatchCommand) {
	defer p.workers.Add(-1)
	for {
		runNode(cmd)
		cmd = nil
		select {
		case cmd = <-p.work:
		case <-p.done:
			return
		}
	}
}

// runNode is the only place a node runs. Every retry finishes inside
// Execute, and Done is its last access to the batch: once Wait returns, no
// goroutine touches b.
func runNode(cmd *fastBatchCommand) {
	b := cmd.fb
	defer b.wg.Done()
	if err := cmd.Execute(); err != nil {
		b.errMu.Lock()
		b.errs = chainErrors(err, b.errs)
		b.errMu.Unlock()
	}
	if b.sem != nil {
		<-b.sem
	}
}

// ExecuteFast runs b with one command per node and fills its results, which
// are read with the FastBatch accessors after it returns. Reads are routed and
// chained like BatchGetComplex, writes like BatchOperate. A node holding a
// single key still gets a batch command.
func (clnt *Client) ExecuteFast(policy *BatchPolicy, b *FastBatch) Error {
	b.beginExecute()
	if len(b.keys) == 0 {
		return nil
	}
	policy = clnt.getUsableBatchPolicy(policy)
	write, err := b.hasWrite()
	if err != nil {
		return err
	}
	if err := clnt.checkFast(policy, b, write); err != nil {
		return err
	}
	if err := clnt.routeFast(policy, b, write); err != nil {
		return err
	}
	if b.nCmds == 0 {
		return newError(types.INVALID_NAMESPACE)
	}

	err = clnt.runFast(policy, b)
	if write || (err != nil && !policy.AllowPartialResults) {
		return err
	}
	filteredOut := 0
	for _, cmd := range b.cmds[:b.nCmds] {
		filteredOut += cmd.filteredOut()
	}
	if filteredOut > 0 {
		err = chainErrors(ErrFilteredOut.err(), err)
	}
	return err
}

func (b *FastBatch) beginExecute() {
	b.nCmds = 0
	b.errs = nil
	b.prepareResults()
}

func (clnt *Client) routeFast(policy *BatchPolicy, b *FastBatch, write bool) Error {
	cluster := clnt.cluster
	if len(cluster.GetNodes()) == 0 {
		return ErrClusterIsEmpty.err()
	}
	replicaSC := GetReplicaPolicySC(policy.GetBasePolicy())

	var errs Error
	for i := range b.keys {
		var node *Node
		var err Error
		if write {
			node, err = GetNodeBatchWrite(cluster, &b.keys[i], policy.ReplicaPolicy, nil, 0)
		} else {
			node, err = GetNodeBatchRead(cluster, &b.keys[i], policy.ReplicaPolicy, replicaSC, nil, 0, 0)
		}
		if err != nil {
			if !write {
				return err
			}
			b.results[i].code, b.results[i].node = err.resultCode(), node
			if !err.Matches(types.INVALID_NAMESPACE) {
				errs = chainErrors(err, errs)
			}
			continue
		}
		cmd := b.cmdOn(node)
		if cmd == nil {
			cmd = b.cmdFor(node)
			cmd.client, cmd.policy = clnt, policy
		}
		cmd.batch.offsets = append(cmd.batch.offsets, i)
	}
	if errs != nil && policy.RespondAllKeys {
		return errs
	}
	return nil
}

func (b *FastBatch) cmdOn(node *Node) *fastBatchCommand {
	for _, cmd := range b.cmds[:b.nCmds] {
		if cmd.node == node {
			return cmd
		}
	}
	return nil
}

// runFast runs every command of b, the last one on the caller, and waits for
// all of them. ConcurrentNodes caps how many run at once, as in batchExecute.
func (clnt *Client) runFast(policy *BatchPolicy, b *FastBatch) Error {
	cmds := b.cmds[:b.nCmds]
	b.sem = nil
	if n := policy.ConcurrentNodes; n > 0 && n < len(cmds) {
		if cap(b.semBuf) != n {
			b.semBuf = make(chan struct{}, n)
		}
		b.sem = b.semBuf
	}

	var pool *fastExec
	if len(cmds) > 1 {
		pool = clnt.fastPool()
	}
	last := len(cmds) - 1
	b.wg.Add(len(cmds))
	for i, cmd := range cmds {
		if b.sem != nil {
			b.sem <- struct{}{}
		}
		if i == last {
			runNode(cmd)
		} else {
			pool.submit(cmd)
		}
	}
	b.wg.Wait()
	return b.errs
}
