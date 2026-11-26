package aerospike

import (
	"bytes"
	"testing"
)

func TestFastBatchDigestMatchesNewKey(t *testing.T) {
	b := NewFastBatch("test", "sess")
	blob := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	i := b.addKeyBlob(blob)
	want, _ := NewKey("test", "sess", blob)
	if !bytes.Equal(want.Digest(), b.keys[i].Digest()) {
		t.Fatalf("blob digest mismatch: want %x got %x", want.Digest(), b.keys[i].Digest())
	}
	j := b.addKeyString("eid:abc")
	want2, _ := NewKey("test", "sess", "eid:abc")
	if !bytes.Equal(want2.Digest(), b.keys[j].Digest()) {
		t.Fatalf("string digest mismatch: want %x got %x", want2.Digest(), b.keys[j].Digest())
	}
	if b.keys[j].Namespace() != "test" || b.keys[j].SetName() != "sess" {
		t.Fatalf("namespace/set not set: %q %q", b.keys[j].Namespace(), b.keys[j].SetName())
	}
	if b.Len() != 2 {
		t.Fatalf("Len = %d, want 2", b.Len())
	}
	b.Reset()
	if b.Len() != 0 {
		t.Fatalf("Len after Reset = %d", b.Len())
	}
}
