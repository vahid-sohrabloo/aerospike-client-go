package aerospike

import (
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

const fastExecSet = "fastexec"

var fastRunID = uint64(time.Now().UnixNano())

func fastNS() string { return flag.Lookup("n").Value.String() }

// fastServerClientPolicy connects to the seed in FASTBATCH_3NODE when set, else to
// the suite's -h/-p server.
func fastServerClient(t testing.TB) *Client {
	t.Helper()
	return fastServerClientPolicy(t, nil)
}

func fastServerClientPolicy(t testing.TB, cp *ClientPolicy) *Client {
	t.Helper()
	host := flag.Lookup("h").Value.String()
	port, _ := strconv.Atoi(flag.Lookup("p").Value.String())
	seed := os.Getenv("FASTBATCH_3NODE")
	if seed != "" {
		h, p, err := net.SplitHostPort(seed)
		if err != nil {
			t.Fatalf("FASTBATCH_3NODE=%q: %v", seed, err)
		}
		host = h
		port, _ = strconv.Atoi(p)
	}
	start := time.Now()
	clnt, err := NewClientWithPolicy(cp, host, port)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clnt.Close)
	for seed != "" && len(clnt.GetNodes()) < 3 && time.Since(start) < 10*time.Second {
		time.Sleep(50 * time.Millisecond)
	}
	// A cold pool fails the first attempt with "pool is empty"; on a
	// replicated cluster the SEQUENCE retry then splits to the prole and can
	// run out of MaxRetries, as BatchGetComplex does too.
	if _, err := clnt.WarmUp(80); err != nil {
		t.Fatal(err)
	}
	return clnt
}

func fastBlobKey(tag byte, i int) []byte {
	k := make([]byte, 0, 17)
	k = binary.BigEndian.AppendUint64(k, fastRunID)
	k = append(k, tag)
	return binary.BigEndian.AppendUint64(k, uint64(i))
}

func fastStringKey(tag byte, i int) string {
	return fmt.Sprintf("eid:%d:%c:%d", fastRunID, tag, i)
}

func fastKey(t testing.TB, v any) *Key {
	t.Helper()
	k, err := NewKey(fastNS(), fastExecSet, v)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func fastAny(v FastValue) any {
	if n, ok := v.Int(); ok {
		return n
	}
	if p, ok := v.Bytes(); ok {
		return string(p)
	}
	if it, ok := v.Map(); ok {
		m := map[any]any{}
		for {
			k, e, ok := it.Next()
			if !ok {
				break
			}
			m[fastAny(k)] = fastAny(e)
		}
		return m
	}
	if it, ok := v.List(); ok {
		l := []any{}
		for {
			e, ok := it.Next()
			if !ok {
				break
			}
			l = append(l, fastAny(e))
		}
		return l
	}
	return fmt.Sprintf("unreadable ptype %d", v.ptype)
}

func officialAny(v any) any {
	switch x := v.(type) {
	case int:
		return int64(x)
	case []byte:
		return string(x)
	case map[any]any:
		m := map[any]any{}
		for k, e := range x {
			m[officialAny(k)] = officialAny(e)
		}
		return m
	case []any:
		l := []any{}
		for _, e := range x {
			l = append(l, officialAny(e))
		}
		return l
	}
	return v
}

func TestFastExecuteRoundTrip(t *testing.T) {
	clnt := fastServerClient(t)
	policy := NewBatchPolicy()
	wp := NewBatchWritePolicy()
	fwp := NewFastWritePolicy(wp)
	mp, lp := NewMapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE), DefaultListPolicy()
	shape := expTestShapes()[0]
	tmpl, err := CompileExpTemplate(shape.build(ExpParam(0)), 1)
	if err != nil {
		t.Fatal(err)
	}

	const n = 64
	for range 2 {
		b := NewFastBatch(fastNS(), fastExecSet)
		records := make([]BatchRecordIfc, n)
		for i := range n {
			k := int64(i % 5)
			w := b.AddWriteBlob(fastBlobKey('f', i), fwp)
			w.MapIncrementInt("bc", mp, k, 200)
			w.MapIncrementInt("rc", mp, k, 300)
			w.ListAppendIntInMapKey("ll", lp, k, MapOrder.UNORDERED, int64(i))
			w.ListTrimInMapKey("ll", k, MapOrder.UNORDERED, -4, 4)
			w.AddInt("n", int64(i))
			w.ExpWrite(shape.bin, tmpl, shape.flags, []int64{k})

			ctx := CtxMapKeyCreate(IntegerValue(k), MapOrder.UNORDERED)
			records[i] = NewBatchWrite(wp, fastKey(t, fastBlobKey('o', i)),
				MapIncrementOp(mp, "bc", IntegerValue(k), 200),
				MapIncrementOp(mp, "rc", IntegerValue(k), 300),
				ListAppendWithPolicyContextOp(lp, "ll", []*CDTContext{ctx}, i),
				ListTrimOp("ll", -4, 4, ctx),
				AddOp(NewBin("n", i)),
				ExpWriteOp(shape.bin, shape.build(ExpIntVal(k)), shape.flags),
			)
		}
		if err := clnt.ExecuteFast(policy, b); err != nil {
			t.Fatal(err)
		}
		for i := range n {
			if rc := b.ResultCode(i); rc != types.OK {
				t.Fatalf("fast record %d: result code %v", i, rc)
			}
		}
		if err := clnt.BatchOperate(policy, records); err != nil {
			t.Fatal(err)
		}
	}

	fastReads := make([]*BatchRead, n)
	offReads := make([]*BatchRead, n)
	for i := range n {
		fastReads[i] = NewBatchRead(nil, fastKey(t, fastBlobKey('f', i)), nil)
		offReads[i] = NewBatchRead(nil, fastKey(t, fastBlobKey('o', i)), nil)
	}
	if err := clnt.BatchGetComplex(policy, fastReads); err != nil {
		t.Fatal(err)
	}
	if err := clnt.BatchGetComplex(policy, offReads); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		f, o := fastReads[i].Record, offReads[i].Record
		if f == nil || o == nil {
			t.Fatalf("record %d missing: fast %v official %v", i, f, o)
		}
		if len(f.Bins) != 4 {
			t.Fatalf("record %d bins = %v", i, f.Bins)
		}
		if !reflect.DeepEqual(f.Bins, o.Bins) {
			t.Fatalf("record %d:\nfast     %v\nofficial %v", i, f.Bins, o.Bins)
		}
	}
}

var (
	fastMapReadBins = []string{"rc", "bc", "bs", "la", "n"}
	fastEIDReadBins = []string{"eids", "empty", "partner", "abtest"}
)

func putFastReadFixtures(t *testing.T, clnt *Client) {
	t.Helper()
	for i := range 4 {
		err := clnt.Put(nil, fastKey(t, fastBlobKey('r', i)), BinMap{
			"rc": map[any]any{1: 5 + i, 2: 7},
			"bc": map[any]any{1: -3, 1 << 40: 9},
			"la": map[any]any{1: []any{1, 2, i}},
			"n":  9 + i,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = clnt.Put(nil, fastKey(t, fastStringKey('r', i)), BinMap{
			"eids":    []byte{1, 2, byte(i)},
			"partner": "p" + strconv.Itoa(i),
			"abtest":  3,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFastExecuteReadMatchesOfficial(t *testing.T) {
	clnt := fastServerClient(t)
	putFastReadFixtures(t, clnt)
	policy := NewBatchPolicy()
	mapBins, eidBins := NewFastBins(fastMapReadBins...), NewFastBins(fastEIDReadBins...)

	b := NewFastBatch(fastNS(), fastExecSet)
	var records []*BatchRead
	for i := range 4 {
		b.AddReadBlob(fastBlobKey('r', i), mapBins)
		records = append(records, NewBatchRead(nil, fastKey(t, fastBlobKey('r', i)), fastMapReadBins))
		b.AddReadString(fastStringKey('r', i), eidBins)
		records = append(records, NewBatchRead(nil, fastKey(t, fastStringKey('r', i)), fastEIDReadBins))
	}
	b.AddReadBlob(fastBlobKey('x', 0), mapBins)
	records = append(records, NewBatchRead(nil, fastKey(t, fastBlobKey('x', 0)), fastMapReadBins))

	if err := clnt.ExecuteFast(policy, b); err != nil {
		t.Fatal(err)
	}
	if err := clnt.BatchGetComplex(policy, records); err != nil {
		t.Fatal(err)
	}
	for i, r := range records {
		if b.ResultCode(i) != r.ResultCode {
			t.Fatalf("record %d: code %v, official %v", i, b.ResultCode(i), r.ResultCode)
		}
		if b.Found(i) != (r.Record != nil) {
			t.Fatalf("record %d: found %v, official %v", i, b.Found(i), r.Record)
		}
		if r.Record == nil {
			continue
		}
		for _, name := range r.BinNames {
			want, ok := r.Record.Bins[name]
			v, got := b.Bin(i, name)
			if ok != got {
				t.Fatalf("record %d bin %q: present %v, official %v", i, name, got, ok)
			}
			if ok && !reflect.DeepEqual(fastAny(v), officialAny(want)) {
				t.Fatalf("record %d bin %q: %#v, official %#v", i, name, fastAny(v), officialAny(want))
			}
		}
	}
	if b.ResultCode(8) != types.KEY_NOT_FOUND_ERROR || b.RecordErr(8) != nil {
		t.Fatalf("missing key: code %v err %v", b.ResultCode(8), b.RecordErr(8))
	}
}

func putFastInts(t testing.TB, clnt *Client, tag byte, n int) {
	t.Helper()
	for i := range n {
		if err := clnt.Put(nil, fastKey(t, fastBlobKey(tag, i)), BinMap{"n": i}); err != nil {
			t.Fatal(err)
		}
		if err := clnt.Put(nil, fastKey(t, fastStringKey(tag, i)), BinMap{"n": 1000 + i}); err != nil {
			t.Fatal(err)
		}
	}
}

func checkFastInt(b *FastBatch, i int, want int64) error {
	if !b.Found(i) {
		return fmt.Errorf("record %d not found: %v", i, b.ResultCode(i))
	}
	v, ok := b.Bin(i, "n")
	if !ok {
		return fmt.Errorf("record %d: no bin n", i)
	}
	if got, _ := v.Int(); got != want {
		return fmt.Errorf("record %d: n = %d, want %d", i, got, want)
	}
	return nil
}

func TestFastExecuteConcurrent(t *testing.T) {
	clnt := fastServerClient(t)
	putFastInts(t, clnt, 'c', 4)
	policy := NewBatchPolicy()
	policy.ConcurrentNodes = 0
	bins := NewFastBins("n")

	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := NewFastBatch(fastNS(), fastExecSet)
			want := make([]int64, 8)
			for it := range 1000 {
				b.Reset()
				for j := range 4 {
					k := (j + it) % 4
					b.AddReadBlob(fastBlobKey('c', k), bins)
					want[2*j] = int64(k)
					b.AddReadString(fastStringKey('c', k), bins)
					want[2*j+1] = int64(1000 + k)
				}
				if err := clnt.ExecuteFast(policy, b); err != nil {
					t.Error(err)
					return
				}
				for i := range want {
					if err := checkFastInt(b, i, want[i]); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
}

// TestFastExecuteNoStraggler sends the first third of the batch to an inactive
// copy of a node: executeAt skips it, and the SEQUENCE retry splits those keys
// back onto the real node inside the same runNode. A pool cap of 1 sends the
// second node to a plain goroutine while the only worker is busy.
func TestFastExecuteNoStraggler(t *testing.T) {
	clnt := fastServerClient(t)
	putFastInts(t, clnt, 's', 9)
	clnt.fastPool().max = 1
	real := clnt.GetNodes()[0]
	fake := &Node{cluster: clnt.cluster}
	bins := NewFastBins("n")

	b := NewFastBatch(fastNS(), fastExecSet)
	fill := func(round int) {
		b.Reset()
		for i := range 9 {
			b.AddReadBlob(fastBlobKey('s', (i+round)%9), bins)
		}
	}
	fill(0)
	round := 0
	for _, concurrent := range []int{0, 1} {
		policy := NewBatchPolicy()
		policy.ConcurrentNodes = concurrent
		// A split clone inherits the parent's send count, so the retry needs
		// headroom above the default 2.
		policy.MaxRetries = 5
		for range 50 {
			b.beginExecute()
			for j, node := range []*Node{fake, real, real} {
				cmd := b.cmdFor(node)
				cmd.client, cmd.policy = clnt, policy
				for i := 3 * j; i < 3*j+3; i++ {
					cmd.batch.offsets = append(cmd.batch.offsets, i)
				}
			}
			if err := clnt.runFast(policy, b); err != nil {
				t.Fatal(err)
			}
			if !b.cmds[0].splitRetry {
				t.Fatal("fake node was not retried")
			}
			for i := range 9 {
				if err := checkFastInt(b, i, int64((i+round)%9)); err != nil {
					t.Fatal(err)
				}
				if n := b.results[i].node; n == nil || n == fake {
					t.Fatalf("record %d answered by %v", i, b.results[i].node)
				}
			}
			round++
			done := make(chan struct{})
			go func() {
				fill(round)
				close(done)
			}()
			<-done
		}
	}
}

func TestFastExecuteEmpty(t *testing.T) {
	clnt := fastServerClient(t)
	b := NewFastBatch(fastNS(), fastExecSet)
	if err := clnt.ExecuteFast(nil, b); err != nil {
		t.Fatal(err)
	}
}

func TestFastExecuteCloseStopsPool(t *testing.T) {
	clnt := fastServerClient(t)
	putFastInts(t, clnt, 'p', 4)
	real := clnt.GetNodes()[0]
	policy := NewBatchPolicy()
	policy.ConcurrentNodes = 0
	b := NewFastBatch(fastNS(), fastExecSet)
	for i := range 4 {
		b.AddReadBlob(fastBlobKey('p', i), NewFastBins("n"))
	}
	b.beginExecute()
	for j := range 4 {
		cmd := b.cmdFor(real)
		cmd.client, cmd.policy = clnt, policy
		cmd.batch.offsets = append(cmd.batch.offsets, j)
	}
	if err := clnt.runFast(policy, b); err != nil {
		t.Fatal(err)
	}
	p := clnt.fastPool()
	if p.workers.Load() == 0 {
		t.Fatal("no workers started")
	}
	clnt.Close()
	for start := time.Now(); p.workers.Load() != 0; {
		if time.Since(start) > time.Second {
			t.Fatalf("%d workers still running after Close", p.workers.Load())
		}
		time.Sleep(time.Millisecond)
	}
}
