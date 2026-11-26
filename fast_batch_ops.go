package aerospike

import (
	"encoding/binary"
	"math"

	"github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

type FastBins struct {
	enc []byte
	n   int
}

func NewFastBins(names ...string) *FastBins {
	bins := &FastBins{n: len(names)}
	for _, name := range names {
		bins.enc = binary.BigEndian.AppendUint32(bins.enc, uint32(len(name)+4))
		bins.enc = append(bins.enc, _READ.op, 0, 0, byte(len(name)))
		bins.enc = append(bins.enc, name...)
	}
	return bins
}

type FastWritePolicy struct {
	attr batchAttr
}

func NewFastWritePolicy(p *BatchWritePolicy) *FastWritePolicy {
	fp := &FastWritePolicy{}
	fp.attr.setBatchWrite(p)
	return fp
}

type fastEntry struct {
	bins    *FastBins
	policy  *FastWritePolicy
	opOff   int
	opLen   int
	opCount int
}

type FastWrite struct {
	b *FastBatch
	i int
}

func (b *FastBatch) AddReadBlob(key []byte, bins *FastBins) int {
	i := b.addKeyBlob(key)
	b.entries = append(b.entries, fastEntry{bins: bins})
	return i
}

func (b *FastBatch) AddReadString(key string, bins *FastBins) int {
	i := b.addKeyString(key)
	b.entries = append(b.entries, fastEntry{bins: bins})
	return i
}

// AddWriteBlob returns a write that is valid only until the next Add* or Reset.
func (b *FastBatch) AddWriteBlob(key []byte, p *FastWritePolicy) *FastWrite {
	i := b.addKeyBlob(key)
	b.entries = append(b.entries, fastEntry{policy: p, opOff: len(b.opBuf)})
	b.cur = FastWrite{b: b, i: i}
	return &b.cur
}

func (w *FastWrite) MapIncrementInt(bin string, mp *MapPolicy, mapKey, delta int64) {
	start := w.beginOp(_MAP_MODIFY, particleType.BLOB, bin)
	buf := append(w.b.opBuf, 0x94)
	buf = appendPackInt(buf, cdtMapOpTypeIncrement)
	buf = appendPackInt(buf, mapKey)
	buf = appendPackInt(buf, delta)
	buf = appendPackInt(buf, int64(mp.attributes.attr))
	w.endOp(buf, start)
}

func (w *FastWrite) ListAppendIntInMapKey(bin string, lp *ListPolicy, mapKey int64, mapOrder mapOrderType, v int64) {
	start := w.beginOp(_CDT_MODIFY, particleType.BLOB, bin)
	buf := appendCtxMapKey(w.b.opBuf, mapKey, mapOrder)
	buf = append(buf, 0x94)
	buf = appendPackInt(buf, _CDT_LIST_APPEND)
	buf = appendPackInt(buf, v)
	buf = appendPackInt(buf, int64(lp.attributes))
	buf = appendPackInt(buf, int64(lp.flags))
	w.endOp(buf, start)
}

func (w *FastWrite) ListTrimInMapKey(bin string, mapKey int64, mapOrder mapOrderType, index, count int) {
	start := w.beginOp(_CDT_MODIFY, particleType.BLOB, bin)
	buf := appendCtxMapKey(w.b.opBuf, mapKey, mapOrder)
	buf = append(buf, 0x93)
	buf = appendPackInt(buf, _CDT_LIST_TRIM)
	buf = appendPackInt(buf, int64(index))
	buf = appendPackInt(buf, int64(count))
	w.endOp(buf, start)
}

func (w *FastWrite) AddInt(bin string, v int64) {
	start := w.beginOp(_ADD, particleType.INTEGER, bin)
	w.endOp(binary.BigEndian.AppendUint64(w.b.opBuf, uint64(v)), start)
}

func (w *FastWrite) beginOp(op OperationType, particle int, bin string) int {
	start := len(w.b.opBuf)
	w.b.opBuf = append(w.b.opBuf, 0, 0, 0, 0, op.op, byte(particle), 0, byte(len(bin)))
	w.b.opBuf = append(w.b.opBuf, bin...)
	return start
}

func (w *FastWrite) endOp(buf []byte, start int) {
	binary.BigEndian.PutUint32(buf[start:], uint32(len(buf)-start-4))
	w.b.opBuf = buf
	e := &w.b.entries[w.i]
	e.opLen = len(buf) - e.opOff
	e.opCount++
}

func appendCtxMapKey(buf []byte, mapKey int64, order mapOrderType) []byte {
	buf = append(buf, 0x93)
	buf = appendPackInt(buf, 0xff)
	buf = append(buf, 0x92)
	buf = appendPackInt(buf, int64(ctxTypeMapKey|order.flag))
	return appendPackInt(buf, mapKey)
}

// appendPackInt must stay byte-identical to packAInt64.
func appendPackInt(buf []byte, v int64) []byte {
	switch {
	case v >= 0 && v < 128:
		return append(buf, byte(v))
	case v >= 0 && v <= math.MaxUint8:
		return append(buf, 0xcc, byte(v))
	case v >= 0 && v <= math.MaxUint16:
		return binary.BigEndian.AppendUint16(append(buf, 0xcd), uint16(v))
	case v >= 0 && v <= math.MaxUint32:
		return binary.BigEndian.AppendUint32(append(buf, 0xce), uint32(v))
	case v >= 0:
		return binary.BigEndian.AppendUint64(append(buf, 0xcf), uint64(v))
	case v >= -32:
		return append(buf, 0xe0|(byte(v)+32))
	case v >= math.MinInt8:
		return append(buf, 0xd0, byte(v))
	case v >= math.MinInt16:
		return binary.BigEndian.AppendUint16(append(buf, 0xd1), uint16(v))
	case v >= math.MinInt32:
		return binary.BigEndian.AppendUint32(append(buf, 0xd2), uint32(v))
	}
	return binary.BigEndian.AppendUint64(append(buf, 0xd3), uint64(v))
}
