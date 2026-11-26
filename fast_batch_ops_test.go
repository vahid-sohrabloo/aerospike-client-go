package aerospike

import (
	"bytes"
	"math"
	"testing"
)

var fastGoldenMapKeys = []int{0, 1, -1, 1 << 31, 1<<32 - 1}

func officialOpBytes(t *testing.T, ops ...*Operation) []byte {
	t.Helper()
	cmd := &baseCommand{}
	cmd.dataBuffer = make([]byte, 4096)
	for _, op := range ops {
		if err := cmd.writeOperationForOperation(op); err != nil {
			t.Fatal(err)
		}
	}
	return cmd.dataBuffer[:cmd.dataOffset]
}

func fastWriteOpBytes(b *FastBatch) []byte {
	e := b.entries[len(b.entries)-1]
	return b.opBuf[e.opOff : e.opOff+e.opLen]
}

func newGoldenWrite() (*FastBatch, *FastWrite) {
	b := NewFastBatch("test", "sess")
	w := b.AddWriteBlob([]byte{1, 2, 3}, NewFastWritePolicy(NewBatchWritePolicy()))
	return b, w
}

func assertFastBytes(t *testing.T, name string, want, got []byte) {
	t.Helper()
	if !bytes.Equal(want, got) {
		t.Fatalf("%s:\nwant %x\ngot  %x", name, want, got)
	}
}

func TestFastOpGolden_MapIncrement(t *testing.T) {
	mp := NewMapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE)
	for _, k := range fastGoldenMapKeys {
		for _, d := range []int{1, 123456789} {
			want := officialOpBytes(t, MapIncrementOp(mp, "bc", IntegerValue(k), d))
			b, w := newGoldenWrite()
			w.MapIncrementInt("bc", mp, int64(k), int64(d))
			assertFastBytes(t, "map increment", want, fastWriteOpBytes(b))
			if b.entries[0].opCount != 1 {
				t.Fatalf("opCount = %d, want 1", b.entries[0].opCount)
			}
		}
	}
}

func TestFastOpGolden_ListAppendInMapKey(t *testing.T) {
	for _, k := range fastGoldenMapKeys {
		for _, v := range []int{1, 2, 3} {
			ctx := []*CDTContext{CtxMapKeyCreate(IntegerValue(k), MapOrder.UNORDERED)}
			want := officialOpBytes(t, ListAppendWithPolicyContextOp(DefaultListPolicy(), "la", ctx, v))
			b, w := newGoldenWrite()
			w.ListAppendIntInMapKey("la", DefaultListPolicy(), int64(k), MapOrder.UNORDERED, int64(v))
			assertFastBytes(t, "list append", want, fastWriteOpBytes(b))
		}
	}
}

func TestFastOpGolden_ListTrim(t *testing.T) {
	for _, k := range fastGoldenMapKeys {
		want := officialOpBytes(t, ListTrimOp("la", -4, 4, CtxMapKeyCreate(IntegerValue(k), MapOrder.UNORDERED)))
		b, w := newGoldenWrite()
		w.ListTrimInMapKey("la", int64(k), MapOrder.UNORDERED, -4, 4)
		assertFastBytes(t, "list trim", want, fastWriteOpBytes(b))
	}
}

func TestFastOpGolden_AddInt(t *testing.T) {
	for _, n := range []int{0, 1, -1, 123456789, 1 << 40} {
		want := officialOpBytes(t, AddOp(NewBin("rc", n)))
		b, w := newGoldenWrite()
		w.AddInt("rc", int64(n))
		assertFastBytes(t, "add", want, fastWriteOpBytes(b))
	}
}

func TestFastOpGolden_Sequence(t *testing.T) {
	mp := NewMapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE)
	ctx := []*CDTContext{CtxMapKeyCreate(IntegerValue(7), MapOrder.UNORDERED)}
	want := officialOpBytes(t,
		MapIncrementOp(mp, "bc", IntegerValue(7), 1),
		ListAppendWithPolicyContextOp(DefaultListPolicy(), "la", ctx, 2),
		ListTrimOp("la", -4, 4, ctx[0]),
		AddOp(NewBin("rc", 1)),
	)
	b, w := newGoldenWrite()
	w.MapIncrementInt("bc", mp, 7, 1)
	w.ListAppendIntInMapKey("la", DefaultListPolicy(), 7, MapOrder.UNORDERED, 2)
	w.ListTrimInMapKey("la", 7, MapOrder.UNORDERED, -4, 4)
	w.AddInt("rc", 1)
	assertFastBytes(t, "sequence", want, fastWriteOpBytes(b))
	if b.entries[0].opCount != 4 {
		t.Fatalf("opCount = %d, want 4", b.entries[0].opCount)
	}

	w2 := b.AddWriteBlob([]byte{9}, NewFastWritePolicy(NewBatchWritePolicy()))
	w2.AddInt("rc", 1)
	if b.entries[1].opOff != len(want) || b.entries[1].opCount != 1 {
		t.Fatalf("second entry = %+v, want opOff %d opCount 1", b.entries[1], len(want))
	}
	assertFastBytes(t, "second entry", officialOpBytes(t, AddOp(NewBin("rc", 1))), fastWriteOpBytes(b))
}

func TestFastOpGolden_ReadBins(t *testing.T) {
	for _, names := range [][]string{{"rc", "bc", "bs", "la"}, {"g1", "gtotal", "hv"}} {
		cmd := &baseCommand{}
		cmd.dataBuffer = make([]byte, 512)
		for _, n := range names {
			if err := cmd.writeOperationForBinName(n, _READ); err != nil {
				t.Fatal(err)
			}
		}
		bins := NewFastBins(names...)
		assertFastBytes(t, "read bins", cmd.dataBuffer[:cmd.dataOffset], bins.enc)
		if bins.n != len(names) {
			t.Fatalf("bins.n = %d, want %d", bins.n, len(names))
		}

		b := NewFastBatch("test", "sess")
		i := b.AddReadBlob([]byte{1, 2, 3}, bins)
		j := b.AddReadString("eid:abc", bins)
		if i != 0 || j != 1 || b.entries[0].bins != bins || b.entries[1].bins != bins {
			t.Fatalf("read entries not recorded: %d %d %+v", i, j, b.entries)
		}
	}
}

func TestFastWritePolicyAttrs(t *testing.T) {
	p := NewBatchWritePolicy()
	p.Expiration = 3600
	p.RecordExistsAction = UPDATE_ONLY
	var want batchAttr
	want.setBatchWrite(p)
	if got := NewFastWritePolicy(p).attr; got != want {
		t.Fatalf("attr = %+v, want %+v", got, want)
	}
}

func TestFastOpPackIntMatchesOfficial(t *testing.T) {
	for _, v := range []int64{
		0, 1, 127, 128, 255, 256, 65535, 65536, 1<<32 - 1, 1 << 32, math.MaxInt64,
		-1, -32, -33, -128, -129, -32768, -32769, math.MinInt32, math.MinInt32 - 1, math.MinInt64,
	} {
		cmd := &baseCommand{}
		cmd.dataBuffer = make([]byte, 16)
		if _, err := packAInt64(cmd, v); err != nil {
			t.Fatal(err)
		}
		assertFastBytes(t, "pack int", cmd.dataBuffer[:cmd.dataOffset], appendPackInt(nil, v))
	}
}
