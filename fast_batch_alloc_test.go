//go:build !race

package aerospike

import (
	"os"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

func TestFastBatchKeysZeroAlloc(t *testing.T) {
	b := NewFastBatch("test", "sess")
	blob := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	b.addKeyBlob(blob)
	b.Reset()
	allocs := testing.AllocsPerRun(100, func() {
		b.Reset()
		for range 8 {
			b.addKeyBlob(blob)
			b.addKeyString("eid:abc")
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %v, want 0", allocs)
	}
}

func TestFastOpsZeroAlloc(t *testing.T) {
	b := NewFastBatch("test", "sess")
	mp := NewMapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE)
	lp := DefaultListPolicy()
	wp := NewFastWritePolicy(NewBatchWritePolicy())
	bins := NewFastBins("rc", "bc", "bs", "la")
	key := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	run := func() {
		b.Reset()
		for range 8 {
			b.AddReadBlob(key, bins)
			b.AddReadString("eid:abc", bins)
		}
		b.Reset()
		w := b.AddWriteBlob(key, wp)
		for bidder := range int64(16) {
			k := bidder<<28 | 0xabcdef
			w.MapIncrementInt("bc", mp, k, 1)
			w.MapIncrementInt("bs", mp, k, 123456789)
			w.MapIncrementInt("rc", mp, k, 1)
			w.MapIncrementInt("wc", mp, k, 1)
			w.ListAppendIntInMapKey("la", lp, k, MapOrder.UNORDERED, 3)
			w.ListTrimInMapKey("la", k, MapOrder.UNORDERED, -4, 4)
			w.AddInt("n", 1)
		}
		for range 7 {
			w.AddInt("g", 123456789)
		}
	}
	run()
	allocs := testing.AllocsPerRun(100, run)
	if allocs != 0 {
		t.Fatalf("allocs = %v, want 0", allocs)
	}
	if got := b.entries[0].opCount; got != 16*7+7 {
		t.Fatalf("opCount = %d, want %d", got, 16*7+7)
	}
}

func TestFastExpWriteZeroAlloc(t *testing.T) {
	b := NewFastBatch("test", "sess")
	wp := NewFastWritePolicy(NewBatchWritePolicy())
	key := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	shapes := expTestShapes()
	tmpls := make([]*ExpTemplate, len(shapes))
	for i, s := range shapes {
		tmpl, err := CompileExpTemplate(s.build(ExpParam(0)), 1)
		if err != nil {
			t.Fatal(err)
		}
		tmpls[i] = tmpl
	}
	params := []int64{0}
	run := func() {
		b.Reset()
		w := b.AddWriteBlob(key, wp)
		for k := range int64(16) {
			params[0] = k<<28 | 0xabcdef
			for i, s := range shapes {
				w.ExpWrite(s.bin, tmpls[i], s.flags, params)
			}
		}
	}
	run()
	allocs := testing.AllocsPerRun(100, run)
	if allocs != 0 {
		t.Fatalf("allocs = %v, want 0", allocs)
	}
	if got := b.entries[0].opCount; got != 16*len(shapes) {
		t.Fatalf("opCount = %d, want %d", got, 16*len(shapes))
	}
}

func TestFastCommandWriteBufferZeroAlloc(t *testing.T) {
	clnt, policy := fastTestClient(), fastTestBatchPolicy()
	node := &Node{}
	key := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	rb := NewFastBatch("test", "sess")
	mapBins, eidBins := NewFastBins(fastTestMapBins...), NewFastBins(fastTestEIDBins...)
	for range 4 {
		rb.AddReadBlob(key, mapBins)
		rb.AddReadString("eid:abc", eidBins)
	}

	wb := NewFastBatch("test", "sess")
	mp := NewMapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE)
	wp := NewFastWritePolicy(fastTestWritePolicy())
	for range 4 {
		w := wb.AddWriteBlob(key, wp)
		w.MapIncrementInt("bc", mp, 7, 1)
		w.ListAppendIntInMapKey("la", DefaultListPolicy(), 7, MapOrder.UNORDERED, 2)
		w.ListTrimInMapKey("la", 7, MapOrder.UNORDERED, -4, 4)
		w.AddInt("n", 1)
	}

	buf := make([]byte, 16)
	for _, b := range []*FastBatch{rb, wb} {
		run := func() {
			b.nCmds = 0
			cmd := b.cmdFor(node)
			cmd.client, cmd.policy = clnt, policy
			for i := range b.Len() {
				cmd.batch.offsets = append(cmd.batch.offsets, i)
			}
			cmd.dataBuffer = buf
			if err := cmd.writeBuffer(cmd); err != nil {
				t.Fatal(err)
			}
			buf = cmd.dataBuffer
		}
		run()
		if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
			t.Fatalf("allocs = %v, want 0", allocs)
		}
	}
}

func TestFastResultParseZeroAlloc(t *testing.T) {
	clnt, policy, node := fastTestClient(), fastTestBatchPolicy(), fastTestNode()
	b, _ := fastReadResultBatch()
	reply := fastReadReply()
	conn := fastTestConn(reply, 1<<12)
	fc := conn.conn.(*fastFakeConn)
	sum := int64(0)
	run := func() {
		fc.r.Reset(reply)
		b.nCmds = 0
		b.prepareResults()
		cmd := b.cmdFor(node)
		cmd.client, cmd.policy = clnt, policy
		for i := range b.Len() {
			cmd.batch.offsets = append(cmd.batch.offsets, i)
		}
		cmd.conn = conn
		if err := cmd.parseResult(cmd, conn); err != nil {
			t.Fatal(err)
		}
		for i := range b.Len() {
			if !b.Found(i) {
				continue
			}
			for _, name := range fastFuzzBinNames {
				v, ok := b.Bin(i, name)
				if !ok {
					continue
				}
				if n, ok := v.Int(); ok {
					sum += n
				}
				if p, ok := v.Bytes(); ok {
					sum += int64(len(p))
				}
				if it, ok := v.Map(); ok {
					for {
						_, e, ok := it.Next()
						if !ok {
							break
						}
						if l, ok := e.List(); ok {
							for {
								x, ok := l.Next()
								if !ok {
									break
								}
								n, _ := x.Int()
								sum += n
							}
						} else {
							n, _ := e.Int()
							sum += n
						}
					}
				}
			}
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("allocs = %v, want 0", allocs)
	}
	if sum == 0 {
		t.Fatal("no values read")
	}
}

// fastExecAllocBatches returns a write batch of nWrites records and a read
// batch of nReads blob plus nReads string keys.
func fastExecAllocBatches(tag byte, nReads, nWrites int) (fill func(), batches []*FastBatch) {
	mp := NewMapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE)
	lp := DefaultListPolicy()
	wp := NewFastWritePolicy(NewBatchWritePolicy())
	mapBins, eidBins := NewFastBins(fastMapReadBins...), NewFastBins(fastEIDReadBins...)
	rb, wb := NewFastBatch(fastNS(), fastExecSet), NewFastBatch(fastNS(), fastExecSet)
	blobs := make([][]byte, max(nReads, nWrites))
	strs := make([]string, nReads)
	for i := range blobs {
		blobs[i] = fastBlobKey(tag, i)
	}
	for i := range strs {
		strs[i] = fastStringKey(tag, i)
	}
	fill = func() {
		rb.Reset()
		wb.Reset()
		for i := range nReads {
			rb.AddReadBlob(blobs[i], mapBins)
			rb.AddReadString(strs[i], eidBins)
		}
		for i := range nWrites {
			w := wb.AddWriteBlob(blobs[i], wp)
			w.MapIncrementInt("bc", mp, 7, 1)
			w.ListAppendIntInMapKey("la", lp, 7, MapOrder.UNORDERED, 2)
			w.ListTrimInMapKey("la", 7, MapOrder.UNORDERED, -4, 4)
			w.AddInt("n", 1)
		}
	}
	return fill, []*FastBatch{wb, rb}
}

func assertFastExecZeroAlloc(t *testing.T, clnt *Client, fill func(), b *FastBatch) {
	t.Helper()
	policy := NewBatchPolicy()
	policy.ConcurrentNodes = 0
	run := func() {
		fill()
		if err := clnt.ExecuteFast(policy, b); err != nil {
			t.Fatal(err)
		}
		for i := range b.Len() {
			if rc := b.ResultCode(i); rc != types.OK && rc != types.KEY_NOT_FOUND_ERROR {
				t.Fatalf("record %d: %v", i, rc)
			}
		}
	}
	for range 100 {
		run()
	}
	if allocs := testing.AllocsPerRun(200, run); allocs != 0 {
		t.Fatalf("allocs = %v, want 0", allocs)
	}
}

func TestFastExecuteZeroAlloc(t *testing.T) {
	clnt := fastServerClient(t)
	fill, batches := fastExecAllocBatches('z', 4, 4)
	for _, b := range batches {
		assertFastExecZeroAlloc(t, clnt, fill, b)
	}
}

func TestFastExecuteMultiNodeZeroAlloc(t *testing.T) {
	if os.Getenv("FASTBATCH_3NODE") == "" {
		t.Skip("FASTBATCH_3NODE is unset; see testdata/fastbatch/compose-3node.yml")
	}
	// AllocsPerRun counts every goroutine, and a tend of three nodes
	// allocates, so the client tends only at connect.
	cp := NewClientPolicy()
	cp.TendInterval = time.Hour
	clnt := fastServerClientPolicy(t, cp)
	if n := len(clnt.GetNodes()); n < 3 {
		t.Fatalf("cluster has %d nodes, want 3", n)
	}
	fill, batches := fastExecAllocBatches('m', 32, 64)
	for _, b := range batches {
		assertFastExecZeroAlloc(t, clnt, fill, b)
		if b.Len() != 64 {
			t.Fatalf("batch has %d keys, want 64", b.Len())
		}
		nodes := map[*Node]bool{}
		for i := range b.Len() {
			nodes[b.results[i].node] = true
		}
		if len(nodes) < 2 || b.nCmds < 2 {
			t.Fatalf("batch used %d nodes (%d commands), want more than one", len(nodes), b.nCmds)
		}
	}
}
