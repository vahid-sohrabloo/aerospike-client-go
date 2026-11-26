package aerospike

import (
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

var (
	fastTestMapBins    = []string{"rc", "bc", "bs", "la"}
	fastTestGlobalBins = []string{"v", "o", "grc", "gbc", "gbs", "gla", "rc", "bc", "bs", "la"}
	fastTestEIDBins    = []string{"eids", "empty", "partner", "abtest"}
)

func fastTestClient() *Client {
	return &Client{
		DefaultBatchReadPolicy:  NewBatchReadPolicy(),
		DefaultBatchWritePolicy: NewBatchWritePolicy(),
	}
}

func fastTestBatchPolicy() *BatchPolicy {
	p := NewBatchPolicy()
	p.ReadModeAP = ReadModeAPOne
	p.ConcurrentNodes = 0
	p.SocketTimeout = 50 * time.Millisecond
	p.TotalTimeout = 100 * time.Millisecond
	return p
}

func fastTestWritePolicy() *BatchWritePolicy {
	p := NewBatchWritePolicy()
	p.Expiration = 30 * 24 * 3600
	p.RecordExistsAction = UPDATE
	return p
}

func fastTestKey(t *testing.T, v any) *Key {
	t.Helper()
	k, err := NewKey("test", "sess", v)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func seqOffsets(n int) []int {
	offs := make([]int, n)
	for i := range offs {
		offs[i] = i
	}
	return offs
}

func wireBytes(buf []byte, n int) []byte {
	out := append([]byte(nil), buf[:n]...)
	copy(out[22:26], []byte{0, 0, 0, 0})
	return out
}

func fastCmdBytes(t *testing.T, b *FastBatch, clnt *Client, policy *BatchPolicy, offsets []int) []byte {
	t.Helper()
	cmd := b.cmdFor(&Node{})
	cmd.client = clnt
	cmd.policy = policy
	cmd.batch.offsets = append(cmd.batch.offsets, offsets...)
	cmd.dataBuffer = make([]byte, 16)
	if err := cmd.writeBuffer(cmd); err != nil {
		t.Fatal(err)
	}
	return wireBytes(cmd.dataBuffer, cmd.dataOffset)
}

type fastReadCase struct {
	key  any
	bins []string
}

func fastReadCases() []fastReadCase {
	blob := func(p byte) []byte { return []byte{p, 1, 2, 3, 4, 5, 6, 7} }
	return []fastReadCase{
		{blob('g'), fastTestGlobalBins},
		{blob('p'), fastTestMapBins},
		{blob('b'), fastTestMapBins},
		{blob('s'), fastTestMapBins},
		{"eid:a", fastTestEIDBins},
		{"eid:b", fastTestEIDBins},
		{"eid:c", fastTestEIDBins},
		{"eid:d", fastTestEIDBins},
	}
}

func fillFastReads(b *FastBatch, cases []fastReadCase, bins map[*string]*FastBins) {
	for _, c := range cases {
		fb := bins[&c.bins[0]]
		switch k := c.key.(type) {
		case []byte:
			b.AddReadBlob(k, fb)
		case string:
			b.AddReadString(k, fb)
		}
	}
}

func fastBinsFor(cases []fastReadCase) map[*string]*FastBins {
	m := map[*string]*FastBins{}
	for _, c := range cases {
		if _, ok := m[&c.bins[0]]; !ok {
			m[&c.bins[0]] = NewFastBins(c.bins...)
		}
	}
	return m
}

func officialReadBytes(t *testing.T, clnt *Client, policy *BatchPolicy, cases []fastReadCase, offsets []int) []byte {
	t.Helper()
	records := make([]*BatchRead, len(cases))
	for i, c := range cases {
		records[i] = NewBatchRead(nil, fastTestKey(t, c.key), c.bins)
	}
	cmd := newBatchIndexCommandGet(clnt, &batchNode{Node: &Node{}, offsets: offsets}, policy, records, true)
	cmd.dataBuffer = make([]byte, 16)
	if err := cmd.writeBuffer(&cmd); err != nil {
		t.Fatal(err)
	}
	return wireBytes(cmd.dataBuffer, cmd.dataOffset)
}

func TestFastCommandGolden_Read(t *testing.T) {
	clnt, policy := fastTestClient(), fastTestBatchPolicy()
	cases := fastReadCases()
	want := officialReadBytes(t, clnt, policy, cases, seqOffsets(len(cases)))

	b := NewFastBatch("test", "sess")
	fillFastReads(b, cases, fastBinsFor(cases))
	assertFastBytes(t, "read batch", want, fastCmdBytes(t, b, clnt, policy, seqOffsets(len(cases))))
}

type fastWriteCase struct {
	key      []byte
	official []*Operation
	fast     func(w *FastWrite)
}

func fastWriteCases(t *testing.T) []fastWriteCase {
	mp := NewMapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE)
	lp := DefaultListPolicy()
	ctx := []*CDTContext{CtxMapKeyCreate(IntegerValue(7), MapOrder.UNORDERED)}
	shape := expTestShapes()[0]
	tmpl, err := CompileExpTemplate(shape.build(ExpParam(0)), 1)
	if err != nil {
		t.Fatal(err)
	}
	return []fastWriteCase{
		{
			key: []byte{'p', 1, 2, 3, 4, 5, 6, 7},
			official: []*Operation{
				MapIncrementOp(mp, "bc", IntegerValue(7), 1),
				ListAppendWithPolicyContextOp(lp, "la", ctx, 2),
				ListTrimOp("la", -4, 4, ctx[0]),
				AddOp(NewBin("n", 1)),
			},
			fast: func(w *FastWrite) {
				w.MapIncrementInt("bc", mp, 7, 1)
				w.ListAppendIntInMapKey("la", lp, 7, MapOrder.UNORDERED, 2)
				w.ListTrimInMapKey("la", 7, MapOrder.UNORDERED, -4, 4)
				w.AddInt("n", 1)
			},
		},
		{
			key: []byte{'b', 1, 2, 3, 4, 5, 6, 7},
			official: []*Operation{
				ExpWriteOp(shape.bin, shape.build(ExpIntVal(7)), shape.flags),
				MapIncrementOp(mp, "rc", IntegerValue(7), 1),
			},
			fast: func(w *FastWrite) {
				w.ExpWrite(shape.bin, tmpl, shape.flags, []int64{7})
				w.MapIncrementInt("rc", mp, 7, 1)
			},
		},
		{
			key:      []byte{'g', 1, 2, 3, 4, 5, 6, 7},
			official: []*Operation{AddOp(NewBin("grc", 1))},
			fast:     func(w *FastWrite) { w.AddInt("grc", 1) },
		},
	}
}

func officialWriteBytes(t *testing.T, clnt *Client, policy *BatchPolicy, wp *BatchWritePolicy, cases []fastWriteCase, offsets []int) []byte {
	t.Helper()
	records := make([]BatchRecordIfc, len(cases))
	for i, c := range cases {
		records[i] = NewBatchWrite(wp, fastTestKey(t, c.key), c.official...)
	}
	cmd := newBatchCommandOperate(clnt, &batchNode{Node: &Node{}, offsets: offsets}, policy, records)
	cmd.dataBuffer = make([]byte, 16)
	if err := cmd.writeBuffer(&cmd); err != nil {
		t.Fatal(err)
	}
	return wireBytes(cmd.dataBuffer, cmd.dataOffset)
}

func fillFastWrites(b *FastBatch, cases []fastWriteCase, fwp *FastWritePolicy) {
	for _, c := range cases {
		c.fast(b.AddWriteBlob(c.key, fwp))
	}
}

func TestFastCommandGolden_Write(t *testing.T) {
	clnt, policy, wp := fastTestClient(), fastTestBatchPolicy(), fastTestWritePolicy()
	cases := fastWriteCases(t)
	want := officialWriteBytes(t, clnt, policy, wp, cases, seqOffsets(len(cases)))

	b := NewFastBatch("test", "sess")
	fillFastWrites(b, cases, NewFastWritePolicy(wp))
	assertFastBytes(t, "write batch", want, fastCmdBytes(t, b, clnt, policy, seqOffsets(len(cases))))
}

func TestFastCommandGolden_Subset(t *testing.T) {
	clnt, policy, wp := fastTestClient(), fastTestBatchPolicy(), fastTestWritePolicy()
	rcases := fastReadCases()
	b := NewFastBatch("test", "sess")
	fillFastReads(b, rcases, fastBinsFor(rcases))
	offsets := []int{1, 4, 7}
	assertFastBytes(t, "read subset", officialReadBytes(t, clnt, policy, rcases, offsets), fastCmdBytes(t, b, clnt, policy, offsets))

	wcases := fastWriteCases(t)
	b = NewFastBatch("test", "sess")
	fillFastWrites(b, wcases, NewFastWritePolicy(wp))
	offsets = []int{0, 2}
	assertFastBytes(t, "write subset", officialWriteBytes(t, clnt, policy, wp, wcases, offsets), fastCmdBytes(t, b, clnt, policy, offsets))
}

func TestFastCommandClone(t *testing.T) {
	clnt, policy := fastTestClient(), fastTestBatchPolicy()
	cases := fastReadCases()
	b := NewFastBatch("test", "sess")
	fillFastReads(b, cases, fastBinsFor(cases))
	cmd := b.cmdFor(&Node{})
	cmd.client, cmd.policy = clnt, policy
	cmd.batch.offsets = append(cmd.batch.offsets, seqOffsets(len(cases))...)

	node := &Node{}
	clone, ok := cmd.cloneBatchCommand(&batchNode{Node: node, offsets: []int{3, 5}}).(*fastBatchCommand)
	if !ok {
		t.Fatalf("clone type %T", clone)
	}
	if clone == cmd || clone.node != node || clone.batch.Node != node {
		t.Fatalf("clone not bound to the new node")
	}
	clone.dataBuffer = make([]byte, 16)
	if err := clone.writeBuffer(clone); err != nil {
		t.Fatal(err)
	}
	assertFastBytes(t, "clone", officialReadBytes(t, clnt, policy, cases, []int{3, 5}), wireBytes(clone.dataBuffer, clone.dataOffset))
	if len(cmd.batch.offsets) != len(cases) {
		t.Fatalf("parent offsets changed: %v", cmd.batch.offsets)
	}
}

func TestFastCommandCmdForResets(t *testing.T) {
	b := NewFastBatch("test", "sess")
	b.AddReadString("eid:a", NewFastBins("eids"))
	n1, n2 := &Node{}, &Node{}
	c1 := b.cmdFor(n1)
	c1.batch.offsets = append(c1.batch.offsets, 0)
	c1.commandSentCounter = 3
	c1.splitRetry = true
	c1.filteredOutCnt = 2
	c2 := b.cmdFor(n2)
	if c1 == c2 {
		t.Fatal("two nodes share one command")
	}

	b.Reset()
	c := b.cmdFor(n2)
	if c != c1 {
		t.Fatal("command slot not reused after Reset")
	}
	if c.node != n2 || c.batch.Node != n2 || c.fb != b || len(c.batch.offsets) != 0 ||
		c.commandSentCounter != 0 || c.splitRetry || c.filteredOutCnt != 0 || !c.isOperation {
		t.Fatalf("command not reset: %+v", c)
	}
}

func TestFastCommandRejectsMixed(t *testing.T) {
	b := NewFastBatch("test", "sess")
	b.AddReadBlob([]byte{1}, NewFastBins("rc"))
	b.AddWriteBlob([]byte{2}, NewFastWritePolicy(fastTestWritePolicy())).AddInt("rc", 1)
	cmd := b.cmdFor(&Node{})
	cmd.client, cmd.policy = fastTestClient(), fastTestBatchPolicy()
	cmd.batch.offsets = append(cmd.batch.offsets, 0, 1)
	cmd.dataBuffer = make([]byte, 16)
	err := cmd.writeBuffer(cmd)
	if err == nil || !err.Matches(types.PARAMETER_ERROR) {
		t.Fatalf("err = %v, want PARAMETER_ERROR", err)
	}
}
