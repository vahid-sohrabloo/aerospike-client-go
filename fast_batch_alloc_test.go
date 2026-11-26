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
