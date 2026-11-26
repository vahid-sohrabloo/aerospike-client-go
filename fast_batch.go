package aerospike

import (
	"github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

type FastBatch struct {
	namespace string
	set       string
	keys      []Key
	hasher    keyWriter
}

func NewFastBatch(namespace, set string) *FastBatch {
	return &FastBatch{namespace: namespace, set: set}
}

func (b *FastBatch) Reset() {
	b.keys = b.keys[:0]
}

func (b *FastBatch) Len() int {
	return len(b.keys)
}

func (b *FastBatch) addKeyBlob(k []byte) int {
	b.beginKey(particleType.BLOB)
	b.hasher.Write(k)
	return b.endKey()
}

func (b *FastBatch) addKeyString(k string) int {
	b.beginKey(particleType.STRING)
	b.hasher.WriteString(k)
	return b.endKey()
}

func (b *FastBatch) beginKey(particleType int) {
	b.hasher.hash.Reset()
	b.hasher.WriteString(b.set)
	b.hasher.WriteByte(byte(particleType))
}

func (b *FastBatch) endKey() int {
	b.keys = append(b.keys, Key{namespace: b.namespace, setName: b.set})
	i := len(b.keys) - 1
	b.hasher.hash.Sum(b.keys[i].digest[:])
	return i
}
