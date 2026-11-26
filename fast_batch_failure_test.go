package aerospike

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

func fastErrShape(e Error) []string {
	if e == nil {
		return nil
	}
	var s []string
	var err error = e
	for err != nil {
		switch ae := err.(type) {
		case *AerospikeError:
			s = append(s, fmt.Sprintf("%v inDoubt=%v iter=%d", ae.ResultCode, ae.InDoubt, ae.Iteration))
		case *constAerospikeError:
			s = append(s, fmt.Sprintf("%v inDoubt=%v iter=%d", ae.ResultCode, ae.InDoubt, ae.Iteration))
		default:
			s = append(s, fmt.Sprintf("%T", err))
		}
		err = errors.Unwrap(err)
	}
	return s
}

func assertSameErr(t *testing.T, err, oerr Error) {
	t.Helper()
	if got, want := fastErrShape(err), fastErrShape(oerr); !slices.Equal(got, want) {
		t.Fatalf("error chain:\nfast     %q (%v)\nofficial %q (%v)", got, err, want, oerr)
	}
}

type fastPair struct {
	b       *FastBatch
	reads   []*BatchRead
	records []BatchRecordIfc
	write   bool
}

func (p *fastPair) addRead(t testing.TB, key []byte, bins []string) {
	p.b.AddReadBlob(key, NewFastBins(bins...))
	p.reads = append(p.reads, NewBatchRead(nil, fastKey(t, key), bins))
}

func (p *fastPair) addWrite(t testing.TB, key []byte, wp *BatchWritePolicy, n int64) {
	p.write = true
	p.b.AddWriteBlob(key, NewFastWritePolicy(wp)).AddInt("n", n)
	p.records = append(p.records, NewBatchWrite(wp, fastKey(t, key), AddOp(NewBin("n", n))))
}

func (p *fastPair) assertRecords(t *testing.T) {
	t.Helper()
	if p.write {
		for i, r := range p.records {
			assertFastMatchesOfficial(t, p.b, r.BatchRec(), i, true)
		}
		return
	}
	for i, r := range p.reads {
		assertFastMatchesOfficial(t, p.b, &r.BatchRecord, i, false)
	}
}

func (p *fastPair) official(clnt *Client, policy *BatchPolicy) Error {
	if p.write {
		return clnt.BatchOperate(policy, p.records)
	}
	return clnt.BatchGetComplex(policy, p.reads)
}

// onNode runs every key of the pair as one command on node, the same way for
// both clients, so a test can pick the node the batch starts on.
func (p *fastPair) onNode(clnt *Client, policy *BatchPolicy, node *Node) (Error, Error) {
	offsets := seqOffsets(p.b.Len())
	p.b.beginExecute()
	cmd := p.b.cmdFor(node)
	cmd.client, cmd.policy = clnt, policy
	cmd.batch.offsets = append(cmd.batch.offsets, offsets...)
	err := clnt.runFast(policy, p.b)

	var ocmd batcher
	if p.write {
		for _, r := range p.records {
			r.prepare()
		}
		c := newBatchCommandOperate(clnt, nil, policy, p.records)
		ocmd = c.cloneBatchCommand(&batchNode{Node: node, offsets: offsets})
	} else {
		for _, r := range p.reads {
			r.prepare()
		}
		c := newBatchIndexCommandGet(clnt, nil, policy, p.reads, true)
		ocmd = c.cloneBatchCommand(&batchNode{Node: node, offsets: offsets})
	}
	return err, ocmd.Execute()
}

func newFastPair() *fastPair {
	return &fastPair{b: NewFastBatch(fastNS(), fastExecSet)}
}

func fastReadPair(t testing.TB, tag byte, n int) *fastPair {
	p := newFastPair()
	for i := range n {
		p.addRead(t, fastBlobKey(tag, i), []string{"n"})
	}
	return p
}

func fastWritePair(t testing.TB, tag byte, n int) *fastPair {
	p := newFastPair()
	wp := NewBatchWritePolicy()
	for i := range n {
		p.addWrite(t, fastBlobKey(tag, i), wp, 1)
	}
	return p
}

func TestFastTimeoutParity(t *testing.T) {
	clnt := fastServerClient(t)
	// 64 keys leave no node with a single key, where the official client
	// runs a single-record command that puts the TIMEOUT on the record.
	putFastInts(t, clnt, 't', 64)
	for _, write := range []bool{false, true} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("write=%v/partial=%v", write, partial), func(t *testing.T) {
				policy := NewBatchPolicy()
				policy.TotalTimeout = time.Microsecond
				policy.AllowPartialResults = partial
				p := fastReadPair(t, 't', 64)
				if write {
					p = fastWritePair(t, 't', 64)
				}
				err := clnt.ExecuteFast(policy, p.b)
				oerr := p.official(clnt, policy)
				if oerr == nil || !oerr.Matches(types.TIMEOUT) {
					t.Fatalf("official err = %v, want TIMEOUT", oerr)
				}
				// Below the outer error, the chain depends on timing: each node
				// adds its errors in completion order, and a node may or may
				// not have tried once before the deadline.
				if err == nil || err.resultCode() != oerr.resultCode() || err.IsInDoubt() != oerr.IsInDoubt() {
					t.Fatalf("err = %v, official %v", err, oerr)
				}
				p.assertRecords(t)
			})
		}
	}
}

func TestFastRejectsFilterExpression(t *testing.T) {
	clnt := fastServerClient(t)
	putFastInts(t, clnt, 'e', 4)
	policy := NewBatchPolicy()
	policy.FilterExpression = ExpEq(ExpIntBin("n"), ExpIntVal(-1))
	filtered := NewBatchWritePolicy()
	filtered.FilterExpression = policy.FilterExpression
	perRecord := fastWritePair(t, 'e', 3)
	perRecord.addWrite(t, fastBlobKey('e', 3), filtered, 1)
	for _, p := range []*fastPair{fastReadPair(t, 'e', 4), fastWritePair(t, 'e', 4), perRecord} {
		policy := policy
		if p == perRecord {
			policy = NewBatchPolicy()
		}
		err := clnt.ExecuteFast(policy, p.b)
		if err == nil || !err.Matches(types.PARAMETER_ERROR) {
			t.Fatalf("write=%v: err = %v, want PARAMETER_ERROR", p.write, err)
		}
		for i := range p.b.Len() {
			if rc := p.b.ResultCode(i); rc != types.NO_RESPONSE || p.b.InDoubt(i) {
				t.Fatalf("write=%v record %d: code %v inDoubt %v", p.write, i, rc, p.b.InDoubt(i))
			}
		}
	}
}

// A negative verbosity encodes as 0 in the official client, so it is accepted.
func TestFastRejectsErrorDetailVerbosity(t *testing.T) {
	clnt := fastServerClient(t)
	putFastInts(t, clnt, 'w', 4)
	for _, verbosity := range []int{-1, 1, 2, 3, 4} {
		for _, p := range []*fastPair{fastReadPair(t, 'w', 4), fastWritePair(t, 'w', 4)} {
			policy := NewBatchPolicy()
			policy.ErrorDetailVerbosity = verbosity
			err := clnt.ExecuteFast(policy, p.b)
			if verbosity < 0 {
				if err != nil {
					t.Fatalf("verbosity=%d write=%v: err = %v", verbosity, p.write, err)
				}
				continue
			}
			if err == nil || !err.Matches(types.PARAMETER_ERROR) {
				t.Fatalf("verbosity=%d write=%v: err = %v, want PARAMETER_ERROR", verbosity, p.write, err)
			}
			for i := range p.b.Len() {
				if rc := p.b.ResultCode(i); rc != types.NO_RESPONSE || p.b.InDoubt(i) {
					t.Fatalf("verbosity=%d write=%v record %d: code %v inDoubt %v", verbosity, p.write, i, rc, p.b.InDoubt(i))
				}
			}
		}
	}
}

// A batch of one key runs the official single-record path, which keeps going
// on a server error and stops on a client error.
func TestFastSingleKeyAbortParity(t *testing.T) {
	clnt := fastServerClient(t)
	if err := clnt.Put(nil, fastKey(t, fastBlobKey('s', 0)), BinMap{"n": "str"}); err != nil {
		t.Fatal(err)
	}
	if _, err := clnt.Delete(nil, fastKey(t, fastBlobKey('s', 1))); err != nil {
		t.Fatal(err)
	}
	badWrite := newFastPair()
	badWrite.addWrite(t, fastBlobKey('s', 0), NewBatchWritePolicy(), 1)
	missingRead := newFastPair()
	missingRead.addRead(t, fastBlobKey('s', 1), []string{"n"})

	for _, p := range []*fastPair{badWrite, missingRead} {
		err := clnt.ExecuteFast(nil, p.b)
		oerr := p.official(clnt, nil)
		if err != nil || oerr != nil {
			t.Fatalf("write=%v: err = %v, official %v; want both nil", p.write, err, oerr)
		}
		p.assertRecords(t)
	}
	if rc := badWrite.b.ResultCode(0); rc != types.BIN_TYPE_ERROR {
		t.Fatalf("bad write: %v", rc)
	}

	policy := NewBatchPolicy()
	policy.TotalTimeout = time.Microsecond
	for _, p := range []*fastPair{badWrite, missingRead} {
		err := clnt.ExecuteFast(policy, p.b)
		oerr := p.official(clnt, policy)
		if oerr == nil || !oerr.Matches(types.TIMEOUT) {
			t.Fatalf("write=%v: official err = %v, want TIMEOUT", p.write, oerr)
		}
		// Below the outer error, the chain and iteration depend on whether an
		// attempt started before the deadline.
		if err == nil || err.resultCode() != oerr.resultCode() || err.IsInDoubt() != oerr.IsInDoubt() {
			t.Fatalf("write=%v: err = %v, official %v", p.write, err, oerr)
		}
	}
}

// OnLockingOnly sets info4 without a transaction, which FastBatch does not
// encode.
func TestFastRejectsTxnAttr(t *testing.T) {
	clnt := fastServerClient(t)
	putFastInts(t, clnt, 'l', 4)
	locking := NewBatchWritePolicy()
	locking.OnLockingOnly = true
	p := fastWritePair(t, 'l', 3)
	p.addWrite(t, fastBlobKey('l', 3), locking, 1)
	err := clnt.ExecuteFast(nil, p.b)
	if err == nil || !err.Matches(types.PARAMETER_ERROR) {
		t.Fatalf("err = %v, want PARAMETER_ERROR", err)
	}
	for i := range p.b.Len() {
		if rc := p.b.ResultCode(i); rc != types.NO_RESPONSE || p.b.InDoubt(i) {
			t.Fatalf("record %d: code %v inDoubt %v", i, rc, p.b.InDoubt(i))
		}
	}
}

// cutProxy forwards each connection to target and closes it after cut reply
// bytes.
type cutProxy struct {
	ln     net.Listener
	target string
	cut    atomic.Int64
}

func newCutProxy(t *testing.T, target string) *cutProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &cutProxy{ln: ln, target: target}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(c)
		}
	}()
	return p
}

func (p *cutProxy) serve(c net.Conn) {
	defer c.Close()
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer up.Close()
	go io.Copy(up, c)
	io.CopyN(c, up, p.cut.Load())
}

func fastProxyNode(t *testing.T, clnt *Client, real *Node) (*Node, *cutProxy) {
	t.Helper()
	proxy := newCutProxy(t, real.host.String())
	addr := proxy.ln.Addr().(*net.TCPAddr)
	node := newNode(clnt.cluster, &nodeValidator{
		name:          real.name + "-cut",
		primaryHost:   NewHost(addr.IP.String(), addr.Port),
		features:      real.features,
		serverVersion: real.serverVersion,
	})
	node.partitionGeneration.Set(0)
	return node, proxy
}

func TestFastPartialReplyNoPanic(t *testing.T) {
	clnt := fastServerClient(t)
	putFastReadFixtures(t, clnt)
	node, proxy := fastProxyNode(t, clnt, clnt.GetNodes()[0])
	policy := NewBatchPolicy()
	policy.MaxRetries = 0

	for _, write := range []bool{false, true} {
		for _, cut := range []int64{4, 12, 30, 60, 100} {
			p := newFastPair()
			for i := range 4 {
				if write {
					p.addWrite(t, fastBlobKey('r', i), NewBatchWritePolicy(), 0)
				} else {
					p.addRead(t, fastBlobKey('r', i), fastMapReadBins)
				}
			}
			proxy.cut.Store(cut)
			for range 2 {
				node.makeConnectionForPool(0)
			}
			err, oerr := p.onNode(clnt, policy, node)
			if err == nil {
				t.Fatalf("write=%v cut=%d: no error", write, cut)
			}
			assertSameErr(t, err, oerr)
			p.assertRecords(t)
		}
	}
}

// TestFastSplitRetryParity starts the whole batch on an inactive copy of a
// node, so the retry splits the keys over the real nodes.
func TestFastSplitRetryParity(t *testing.T) {
	clnt := fastServerClient(t)
	putFastInts(t, clnt, 'v', 16)
	if err := clnt.Put(nil, fastKey(t, fastBlobKey('v', 100)), BinMap{"n": "str"}); err != nil {
		t.Fatal(err)
	}
	fake := &Node{cluster: clnt.cluster}
	policy := NewBatchPolicy()
	policy.MaxRetries = 5

	read := fastReadPair(t, 'v', 16)
	read.addRead(t, fastBlobKey('x', 0), []string{"n"})
	write := fastWritePair(t, 'v', 16)
	write.addWrite(t, fastBlobKey('v', 100), NewBatchWritePolicy(), 1)

	for _, p := range []*fastPair{read, write} {
		err, oerr := p.onNode(clnt, policy, fake)
		assertSameErr(t, err, oerr)
		if !p.b.cmds[0].splitRetry {
			t.Fatalf("write=%v: no split retry", p.write)
		}
		p.assertRecords(t)
		nodes := map[*Node]bool{}
		for i := range p.b.Len() {
			nodes[p.b.results[i].node] = true
		}
		if nodes[fake] || nodes[nil] {
			t.Fatalf("write=%v: a record was answered by the fake node", p.write)
		}
		if want := min(3, len(clnt.GetNodes())); len(nodes) < want {
			t.Fatalf("write=%v: split over %d nodes, want %d", p.write, len(nodes), want)
		}
	}
	last := read.b.Len() - 1
	if read.b.ResultCode(last) != types.KEY_NOT_FOUND_ERROR {
		t.Fatalf("missing key: %v", read.b.ResultCode(last))
	}
	if write.b.ResultCode(last) != types.BIN_TYPE_ERROR {
		t.Fatalf("string bin: %v", write.b.ResultCode(last))
	}
}

func fast3NodeClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("FASTBATCH_3NODE") == "" {
		t.Skip("FASTBATCH_3NODE is unset; see testdata/fastbatch/compose-3node.yml")
	}
	cp := NewClientPolicy()
	cp.TendInterval = time.Hour
	clnt := fastServerClientPolicy(t, cp)
	if n := len(clnt.GetNodes()); n != 3 {
		t.Fatalf("cluster has %d nodes, want 3", n)
	}
	return clnt
}

// fastKeysByNode returns, per node, the indexes of tag keys whose read and
// write both route there.
func fastKeysByNode(t *testing.T, clnt *Client, tag byte, n int) map[*Node][]int {
	t.Helper()
	policy := NewBatchPolicy()
	byNode := map[*Node][]int{}
	for i := range n {
		key := fastKey(t, fastBlobKey(tag, i))
		rn, err := GetNodeBatchRead(clnt.cluster, key, policy.ReplicaPolicy, GetReplicaPolicySC(policy.GetBasePolicy()), nil, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		wn, err := GetNodeBatchWrite(clnt.cluster, key, policy.ReplicaPolicy, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if rn == wn {
			byNode[rn] = append(byNode[rn], i)
		}
	}
	return byNode
}

func TestFastSingleKeyNodeParity(t *testing.T) {
	clnt := fast3NodeClient(t)
	byNode := fastKeysByNode(t, clnt, 'g', 64)
	missing := fastKeysByNode(t, clnt, 'h', 64)
	nodes := clnt.GetNodes()
	var others []int
	for _, node := range nodes[1:] {
		others = append(others, byNode[node][:4]...)
	}
	good, bad := byNode[nodes[0]][0], byNode[nodes[0]][1]
	for _, i := range append(others, good) {
		if err := clnt.Put(nil, fastKey(t, fastBlobKey('g', i)), BinMap{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := clnt.Put(nil, fastKey(t, fastBlobKey('g', bad)), BinMap{"n": "str"}); err != nil {
		t.Fatal(err)
	}
	policy := NewBatchPolicy()
	wp := NewBatchWritePolicy()

	for _, odd := range []bool{false, true} {
		read, write := newFastPair(), newFastPair()
		for _, i := range others {
			read.addRead(t, fastBlobKey('g', i), []string{"n"})
			write.addWrite(t, fastBlobKey('g', i), wp, 1)
		}
		if odd {
			read.addRead(t, fastBlobKey('h', missing[nodes[0]][0]), []string{"n"})
			write.addWrite(t, fastBlobKey('g', bad), wp, 1)
		} else {
			read.addRead(t, fastBlobKey('g', good), []string{"n"})
			write.addWrite(t, fastBlobKey('g', good), wp, 1)
		}

		for _, p := range []*fastPair{read, write} {
			err := clnt.ExecuteFast(policy, p.b)
			oerr := p.official(clnt, policy)
			singles := 0
			for _, cmd := range p.b.cmds[:p.b.nCmds] {
				if len(cmd.batch.offsets) == 1 {
					singles++
				}
			}
			if p.b.nCmds != 3 || singles != 1 {
				t.Fatalf("write=%v: %d commands, %d with one key; want 3 and 1", p.write, p.b.nCmds, singles)
			}
			assertSameErr(t, err, oerr)
			p.assertRecords(t)
			want := types.OK
			switch {
			case odd && p.write:
				want = types.BIN_TYPE_ERROR
			case odd:
				want = types.KEY_NOT_FOUND_ERROR
			}
			if rc := p.b.ResultCode(p.b.Len() - 1); rc != want {
				t.Fatalf("write=%v odd=%v: single key code %v, want %v", p.write, odd, rc, want)
			}
		}
	}
}

// TestFastNodeDownParity stops as3 of the compose cluster. The client tends
// only at connect, so both clients keep routing to the stopped node.
func TestFastNodeDownParity(t *testing.T) {
	clnt := fast3NodeClient(t)
	var down *Node
	for _, node := range clnt.GetNodes() {
		if node.host.Port == 3120 {
			down = node
		}
	}
	if down == nil {
		t.Fatal("no node on port 3120")
	}
	if len(fastKeysByNode(t, clnt, 'd', 64)[down]) == 0 {
		t.Fatal("no key on the stopped node")
	}
	putFastInts(t, clnt, 'd', 64)
	compose := func(args ...string) {
		args = append([]string{"compose", "-f", "testdata/fastbatch/compose-3node.yml"}, args...)
		if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
			t.Errorf("docker %v: %v: %s", args, err, out)
		}
	}
	compose("stop", "as3")
	// A node that rejoins and takes migrations runs out of write blocks
	// with this small data-size, so the whole cluster starts fresh.
	t.Cleanup(func() { compose("up", "-d", "--force-recreate") })
	// Writes stall on the server until the other two nodes drop as3, and a
	// call made in that window would see a different failure than the next.
	for _, node := range clnt.GetNodes() {
		for start := time.Now(); node != down; time.Sleep(50 * time.Millisecond) {
			info, err := node.RequestInfo(NewInfoPolicy(), "statistics")
			if err == nil && strings.Contains(info["statistics"], "cluster_size=2;") {
				break
			}
			if time.Since(start) > 10*time.Second {
				t.Fatalf("%v still sees as3: %v", node, err)
			}
		}
	}

	for _, retries := range []int{0, 2} {
		policy := NewBatchPolicy()
		policy.MaxRetries = retries
		for _, p := range []*fastPair{fastReadPair(t, 'd', 64), fastWritePair(t, 'd', 64)} {
			oerr := p.official(clnt, policy)
			err := clnt.ExecuteFast(policy, p.b)
			assertSameErr(t, err, oerr)
			p.assertRecords(t)
			if retries == 0 && oerr == nil {
				t.Fatalf("write=%v: no error with the node down", p.write)
			}
		}
	}
}
