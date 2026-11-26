//go:build !race

package aerospike

import "testing"

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
