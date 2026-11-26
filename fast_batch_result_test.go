package aerospike

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

type fastFakeConn struct{ r bytes.Reader }

func (c *fastFakeConn) Read(p []byte) (int, error)       { return c.r.Read(p) }
func (c *fastFakeConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *fastFakeConn) Close() error                     { return nil }
func (c *fastFakeConn) LocalAddr() net.Addr              { return nil }
func (c *fastFakeConn) RemoteAddr() net.Addr             { return nil }
func (c *fastFakeConn) SetDeadline(time.Time) error      { return nil }
func (c *fastFakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fastFakeConn) SetWriteDeadline(time.Time) error { return nil }
func fastTestConn(reply []byte, bufSize int) *Connection {
	fc := &fastFakeConn{}
	fc.r.Reset(reply)
	return &Connection{conn: fc, dataBuffer: make([]byte, bufSize), limitReader: &io.LimitedReader{R: fc}}
}

type fastReplyOp struct {
	name  string
	ptype byte
	val   []byte
}

type fastReplyRec struct {
	code   types.ResultCode
	index  uint32
	info3  byte
	fields [][]byte
	ops    []fastReplyOp
}

func fastReplyLast() fastReplyRec { return fastReplyRec{info3: byte(_INFO3_LAST)} }

func fastReplyMsg(recs ...fastReplyRec) []byte {
	var body []byte
	for _, r := range recs {
		body = append(body, 22, 0, 0, r.info3, 0, byte(r.code))
		body = binary.BigEndian.AppendUint32(body, 7)
		body = binary.BigEndian.AppendUint32(body, 1000)
		body = binary.BigEndian.AppendUint32(body, r.index)
		body = binary.BigEndian.AppendUint16(body, uint16(len(r.fields)))
		body = binary.BigEndian.AppendUint16(body, uint16(len(r.ops)))
		for _, f := range r.fields {
			body = binary.BigEndian.AppendUint32(body, uint32(len(f)))
			body = append(body, f...)
		}
		for _, op := range r.ops {
			body = binary.BigEndian.AppendUint32(body, uint32(4+len(op.name)+len(op.val)))
			body = append(body, 1, op.ptype, 0, byte(len(op.name)))
			body = append(body, op.name...)
			body = append(body, op.val...)
		}
	}
	return fastProtoMsg(body)
}

func fastProtoMsg(body []byte) []byte {
	return append(binary.BigEndian.AppendUint64(nil, 2<<56|uint64(_AS_MSG_TYPE)<<48|uint64(len(body))), body...)
}

func opInt(name string, v int64) fastReplyOp {
	return fastReplyOp{name, particleType.INTEGER, binary.BigEndian.AppendUint64(nil, uint64(v))}
}

func opStr(name, v string) fastReplyOp {
	return fastReplyOp{name, particleType.STRING, []byte(v)}
}

func opBlob(name string, v []byte) fastReplyOp { return fastReplyOp{name, particleType.BLOB, v} }

func opMap(name string, v []byte) fastReplyOp { return fastReplyOp{name, particleType.MAP, v} }

func opNull(name string) fastReplyOp { return fastReplyOp{name, particleType.NULL, nil} }

func packIntMap(ordered bool, keys []int64, vals [][]byte) []byte {
	n := len(keys)
	if ordered {
		n++
	}
	var buf []byte
	if n < 16 {
		buf = append(buf, 0x80|byte(n))
	} else {
		buf = binary.BigEndian.AppendUint16(append(buf, 0xde), uint16(n))
	}
	if ordered {
		buf = append(buf, 0xc7, 0, 1, 0xc0)
	}
	for i, k := range keys {
		buf = appendPackInt(buf, k)
		buf = append(buf, vals[i]...)
	}
	return buf
}

func packInts(vs ...int64) []byte {
	buf := []byte{0x90 | byte(len(vs))}
	for _, v := range vs {
		buf = appendPackInt(buf, v)
	}
	return buf
}

func packOne(v int64) []byte { return appendPackInt(nil, v) }

func fastCanonOfficial(v any) any {
	switch v := v.(type) {
	case nil:
		return nil
	case int:
		return int64(v)
	case int64:
		return v
	case string:
		return "b:" + v
	case []byte:
		return "b:" + string(v)
	case []any:
		out := make([]any, len(v))
		for i := range v {
			out[i] = fastCanonOfficial(v[i])
		}
		return out
	case map[any]any:
		out := map[any]any{}
		for k, e := range v {
			out[fastCanonOfficial(k)] = fastCanonOfficial(e)
		}
		return out
	case []MapPair:
		out := map[any]any{}
		for _, p := range v {
			out[fastCanonOfficial(p.Key)] = fastCanonOfficial(p.Value)
		}
		return out
	case OpResults:
		return fastCanonOfficial(v[0])
	}
	return fmt.Sprintf("unsupported %T", v)
}

func fastCanon(t *testing.T, v FastValue) any {
	t.Helper()
	if n, ok := v.Int(); ok {
		return n
	}
	if b, ok := v.Bytes(); ok {
		return "b:" + string(b)
	}
	if it, ok := v.Map(); ok {
		out := map[any]any{}
		for {
			k, e, ok := it.Next()
			if !ok {
				break
			}
			out[fastCanon(t, k)] = fastCanon(t, e)
		}
		if err := it.Err(); err != nil {
			t.Fatalf("map iter: %v", err)
		}
		return out
	}
	if it, ok := v.List(); ok {
		out := []any{}
		for {
			e, ok := it.Next()
			if !ok {
				break
			}
			out = append(out, fastCanon(t, e))
		}
		if err := it.Err(); err != nil {
			t.Fatalf("list iter: %v", err)
		}
		return out
	}
	return nil
}

func fastTestNode() *Node { return &Node{cluster: &Cluster{}} }

func fastParse(b *FastBatch, clnt *Client, policy *BatchPolicy, node *Node, offsets []int, sent int, reply []byte, bufSize int) (*fastBatchCommand, Error) {
	b.prepareResults()
	cmd := b.cmdFor(node)
	cmd.client, cmd.policy = clnt, policy
	cmd.batch.offsets = append(cmd.batch.offsets, offsets...)
	cmd.commandSentCounter = sent
	conn := fastTestConn(reply, bufSize)
	cmd.conn = conn
	return cmd, cmd.parseResult(cmd, conn)
}

func fastReadReply() []byte {
	digest := append([]byte{byte(DIGEST_RIPE)}, make([]byte, 20)...)
	rc := packIntMap(true, []int64{1, 70000, 0xffffffff}, [][]byte{packOne(5), packOne(-2), packOne(300)})
	bs := packIntMap(false, []int64{3, 9}, [][]byte{packInts(1, 2, -3, 1<<40), packInts(0, 0, 0, 0)})
	msg1 := fastReplyMsg(
		fastReplyRec{index: 0, fields: [][]byte{digest}, ops: []fastReplyOp{
			opMap("rc", rc), opMap("bs", bs), opInt("grc", 5), opInt("gla", -3),
		}},
		fastReplyRec{index: 1, ops: []fastReplyOp{
			opBlob("eids", []byte(`{"a":1}`)), opInt("empty", 0), opStr("partner", "p1"), opStr("abtest", ""),
		}},
		fastReplyRec{index: 2, code: types.KEY_NOT_FOUND_ERROR},
	)
	msg2 := fastReplyMsg(
		fastReplyRec{index: 4},
		fastReplyRec{index: 3, code: types.FILTERED_OUT},
		fastReplyLast(),
	)
	return append(msg1, msg2...)
}

func fastReadResultBatch() (*FastBatch, func(t *testing.T) []*BatchRead) {
	b := NewFastBatch("test", "sess")
	mapBins, eidBins := NewFastBins(fastTestMapBins...), NewFastBins(fastTestEIDBins...)
	keys := []any{[]byte{1}, "eid:a", []byte{2}, []byte{3}, []byte{4}, "eid:b"}
	for _, k := range keys {
		switch k := k.(type) {
		case []byte:
			b.AddReadBlob(k, mapBins)
		case string:
			b.AddReadString(k, eidBins)
		}
	}
	return b, func(t *testing.T) []*BatchRead {
		records := make([]*BatchRead, len(keys))
		for i, k := range keys {
			records[i] = NewBatchRead(nil, fastTestKey(t, k), fastTestMapBins)
			records[i].prepare()
		}
		return records
	}
}

func officialParseRead(t *testing.T, clnt *Client, policy *BatchPolicy, node *Node, records []*BatchRead, offsets []int, reply []byte) (*batchIndexCommandGet, Error) {
	t.Helper()
	cmd := newBatchIndexCommandGet(clnt, &batchNode{Node: node, offsets: offsets}, policy, records, true)
	cmd.attr = &batchAttr{}
	conn := fastTestConn(reply, 64)
	cmd.conn = conn
	return &cmd, cmd.parseResult(&cmd, conn)
}

func officialParseWrite(t *testing.T, clnt *Client, policy *BatchPolicy, node *Node, records []BatchRecordIfc, offsets []int, sent int, reply []byte) (*batchCommandOperate, Error) {
	t.Helper()
	cmd := newBatchCommandOperate(clnt, &batchNode{Node: node, offsets: offsets}, policy, records)
	cmd.attr = &batchAttr{hasWrite: true}
	cmd.commandSentCounter = sent
	conn := fastTestConn(reply, 64)
	cmd.conn = conn
	return &cmd, cmd.parseResult(&cmd, conn)
}

func assertFastMatchesOfficial(t *testing.T, b *FastBatch, br *BatchRecord, i int, write bool) {
	t.Helper()
	if got, want := b.ResultCode(i), br.ResultCode; got != want {
		t.Fatalf("entry %d: ResultCode = %v, want %v", i, got, want)
	}
	if got, want := b.InDoubt(i), br.InDoubt; got != want {
		t.Fatalf("entry %d: InDoubt = %v, want %v", i, got, want)
	}
	if got, want := b.Found(i), br.Record != nil; got != want {
		t.Fatalf("entry %d: Found = %v, want %v", i, got, want)
	}
	err := b.RecordErr(i)
	switch {
	case !write && br.ResultCode == types.KEY_NOT_FOUND_ERROR:
		if err != nil {
			t.Fatalf("entry %d: RecordErr = %v, want nil on a read KEY_NOT_FOUND", i, err)
		}
	case br.Err == nil:
		if err != nil {
			t.Fatalf("entry %d: RecordErr = %v, want nil", i, err)
		}
	default:
		if err == nil || !err.Matches(br.ResultCode) || err.IsInDoubt() != br.Err.IsInDoubt() {
			t.Fatalf("entry %d: RecordErr = %v, want %v", i, err, br.Err)
		}
	}
	if br.Record == nil {
		if _, ok := b.Bin(i, "rc"); ok {
			t.Fatalf("entry %d: Bin found on a record without bins", i)
		}
		return
	}
	for name, want := range br.Record.Bins {
		v, ok := b.Bin(i, name)
		if !ok {
			t.Fatalf("entry %d: bin %q missing", i, name)
		}
		if got, want := fastCanon(t, v), fastCanonOfficial(want); !reflect.DeepEqual(got, want) {
			t.Fatalf("entry %d bin %q: got %#v, want %#v", i, name, got, want)
		}
	}
	if _, ok := b.Bin(i, "missing"); ok {
		t.Fatalf("entry %d: Bin(missing) found", i)
	}
}

func TestFastResultParseRead(t *testing.T) {
	clnt, policy, node := fastTestClient(), fastTestBatchPolicy(), fastTestNode()
	reply := fastReadReply()
	b, official := fastReadResultBatch()
	offsets := seqOffsets(b.Len())

	cmd, err := fastParse(b, clnt, policy, node, offsets, 1, reply, 64)
	if err != nil {
		t.Fatal(err)
	}
	records := official(t)
	ocmd, oerr := officialParseRead(t, clnt, policy, node, records, offsets, reply)
	if oerr != nil {
		t.Fatal(oerr)
	}
	if cmd.filteredOut() != ocmd.filteredOut() || cmd.filteredOut() != 1 {
		t.Fatalf("filteredOut = %d, official %d", cmd.filteredOut(), ocmd.filteredOut())
	}
	for i, r := range records {
		assertFastMatchesOfficial(t, b, &r.BatchRecord, i, false)
	}
	if b.ResultCode(5) != types.NO_RESPONSE || b.Found(5) {
		t.Fatalf("entry 5 should have no response")
	}
	if !b.Found(4) {
		t.Fatal("entry 4 (no bins) should be found")
	}

	v, _ := b.Bin(0, "rc")
	if _, ok := v.Int(); ok {
		t.Fatal("Int on a map bin")
	}
	if _, ok := v.Bytes(); ok {
		t.Fatal("Bytes on a map bin")
	}
	if _, ok := v.List(); ok {
		t.Fatal("List on a map bin")
	}
	v, _ = b.Bin(0, "grc")
	if _, ok := v.Map(); ok {
		t.Fatal("Map on an int bin")
	}
}

func TestFastResultParseWrite(t *testing.T) {
	clnt, policy, node := fastTestClient(), fastTestBatchPolicy(), fastTestNode()
	wp := fastTestWritePolicy()
	fwp := NewFastWritePolicy(wp)
	b := NewFastBatch("test", "sess")
	var records []BatchRecordIfc
	for i := range 4 {
		b.AddWriteBlob([]byte{byte(i)}, fwp).AddInt("n", 1)
		records = append(records, NewBatchWrite(wp, fastTestKey(t, []byte{byte(i)}), AddOp(NewBin("n", 1))))
		records[i].BatchRec().prepare()
	}
	reply := fastReplyMsg(
		fastReplyRec{index: 0, ops: []fastReplyOp{
			opInt("rc", 6), opInt("la", 3), opInt("la", 1), opNull("n"),
		}},
		fastReplyRec{index: 1, code: types.OP_NOT_APPLICABLE},
		fastReplyRec{index: 2, code: types.FILTERED_OUT},
		fastReplyLast(),
	)
	offsets := seqOffsets(b.Len())

	for _, sent := range []int{1, 2} {
		t.Run(strconv.Itoa(sent), func(t *testing.T) {
			for _, r := range records {
				r.BatchRec().prepare()
			}
			cmd, err := fastParse(b, clnt, policy, node, offsets, sent, reply, 64)
			if err != nil {
				t.Fatal(err)
			}
			ocmd, oerr := officialParseWrite(t, clnt, policy, node, records, offsets, sent, reply)
			if oerr != nil {
				t.Fatal(oerr)
			}
			cmd.setInDoubt(cmd)
			ocmd.setInDoubt(ocmd)
			if cmd.filteredOut() != ocmd.filteredOut() {
				t.Fatalf("filteredOut = %d, official %d", cmd.filteredOut(), ocmd.filteredOut())
			}
			for i, r := range records {
				assertFastMatchesOfficial(t, b, r.BatchRec(), i, true)
			}
			if !b.InDoubt(3) || b.ResultCode(3) != types.NO_RESPONSE {
				t.Fatal("unanswered write must be in doubt")
			}
		})
	}
}

func TestFastResultReadNotInDoubt(t *testing.T) {
	b, _ := fastReadResultBatch()
	cmd, err := fastParse(b, fastTestClient(), fastTestBatchPolicy(), fastTestNode(), seqOffsets(b.Len()), 2, fastReadReply(), 64)
	if err != nil {
		t.Fatal(err)
	}
	cmd.inDoubt()
	for i := range b.Len() {
		if b.InDoubt(i) {
			t.Fatalf("read entry %d in doubt", i)
		}
	}
}

func TestFastResultCommandError(t *testing.T) {
	b, official := fastReadResultBatch()
	reply := fastReplyMsg(fastReplyRec{code: types.PARAMETER_ERROR, info3: byte(_INFO3_LAST)})
	clnt, policy, node := fastTestClient(), fastTestBatchPolicy(), fastTestNode()
	_, err := fastParse(b, clnt, policy, node, seqOffsets(b.Len()), 1, reply, 64)
	_, oerr := officialParseRead(t, clnt, policy, node, official(t), seqOffsets(b.Len()), reply)
	if err == nil || oerr == nil || !err.Matches(types.PARAMETER_ERROR) || !oerr.Matches(types.PARAMETER_ERROR) {
		t.Fatalf("err = %v, official %v; want PARAMETER_ERROR", err, oerr)
	}
	for i := range b.Len() {
		if b.ResultCode(i) != types.NO_RESPONSE {
			t.Fatalf("entry %d got a result from a command error", i)
		}
	}
}

func TestFastResultBadIndex(t *testing.T) {
	b, _ := fastReadResultBatch()
	reply := fastReplyMsg(fastReplyRec{index: 99}, fastReplyLast())
	_, err := fastParse(b, fastTestClient(), fastTestBatchPolicy(), fastTestNode(), seqOffsets(b.Len()), 1, reply, 64)
	if err == nil || !err.Matches(types.PARSE_ERROR) {
		t.Fatalf("err = %v, want PARSE_ERROR", err)
	}
}

func TestFastResultTruncated(t *testing.T) {
	reply := fastReadReply()
	for n := 0; n < len(reply); n++ {
		b, _ := fastReadResultBatch()
		if _, err := fastParse(b, fastTestClient(), fastTestBatchPolicy(), fastTestNode(), seqOffsets(b.Len()), 1, reply[:n], 64); err == nil {
			t.Fatalf("truncated at %d: no error", n)
		}
	}
}

func TestFastResultViewsSurviveLaterRecords(t *testing.T) {
	b, _ := fastReadResultBatch()
	if _, err := fastParse(b, fastTestClient(), fastTestBatchPolicy(), fastTestNode(), seqOffsets(b.Len()), 1, fastReadReply(), 64); err != nil {
		t.Fatal(err)
	}
	v, _ := b.Bin(1, "eids")
	got, _ := v.Bytes()
	if string(got) != `{"a":1}` {
		t.Fatalf("eids = %q", got)
	}
	b.cmds[0].resp = append(b.cmds[0].resp, make([]byte, 1<<16)...)
	if string(got) != `{"a":1}` {
		t.Fatalf("view changed after the buffer grew: %q", got)
	}
}

func TestFastResultWriteRetrySkipsAnswered(t *testing.T) {
	h, p := flag.Lookup("h"), flag.Lookup("p")
	if h == nil || p == nil {
		t.Skip("no server flags")
	}
	port, _ := strconv.Atoi(p.Value.String())
	clnt, err := NewClient(h.Value.String(), port)
	if err != nil {
		t.Skip(err)
	}
	defer clnt.Close()
	node := clnt.Cluster().GetNodes()[0]

	for _, write := range []bool{true, false} {
		b := NewFastBatch("test", "sess")
		for i := range 4 {
			if write {
				b.AddWriteBlob([]byte{byte(i)}, NewFastWritePolicy(fastTestWritePolicy())).AddInt("n", 1)
			} else {
				b.AddReadBlob([]byte{byte(i)}, NewFastBins("n"))
			}
		}
		b.prepareResults()
		b.results[0].code = types.OK
		b.results[1].code = types.OP_NOT_APPLICABLE
		cmd := b.cmdFor(node)
		cmd.client, cmd.policy = clnt, fastTestBatchPolicy()
		cmd.batch.offsets = append(cmd.batch.offsets, 0, 1, 2, 3)
		nodes, err := cmd.generateBatchNodes(clnt.Cluster())
		if err != nil {
			t.Fatal(err)
		}
		var got []int
		for _, bn := range nodes {
			got = append(got, bn.offsets...)
		}
		slices.Sort(got)
		want := []int{0, 1, 2, 3}
		if write {
			want = []int{2, 3}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("write=%v: retried offsets %v, want %v", write, got, want)
		}
	}
}

func TestFastResultResetClearsResults(t *testing.T) {
	b, _ := fastReadResultBatch()
	if _, err := fastParse(b, fastTestClient(), fastTestBatchPolicy(), fastTestNode(), seqOffsets(b.Len()), 1, fastReadReply(), 64); err != nil {
		t.Fatal(err)
	}
	b.Reset()
	b.AddReadBlob([]byte{1}, NewFastBins("rc"))
	b.prepareResults()
	if b.Found(0) || b.ResultCode(0) != types.NO_RESPONSE {
		t.Fatal("result survived Reset")
	}
}
