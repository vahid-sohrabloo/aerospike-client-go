package aerospike

import (
	"encoding/binary"

	"github.com/aerospike/aerospike-client-go/v8/types"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

type fastResult struct {
	code    types.ResultCode
	inDoubt bool
	found   bool
	node    *Node
	ops     []byte
}

func (b *FastBatch) prepareResults() {
	n := len(b.keys)
	if cap(b.results) < n {
		b.results = make([]fastResult, n)
	}
	b.results = b.results[:n]
	for i := range b.results {
		b.results[i] = fastResult{code: types.NO_RESPONSE}
	}
}

func (b *FastBatch) ResultCode(i int) types.ResultCode { return b.results[i].code }

func (b *FastBatch) InDoubt(i int) bool { return b.results[i].inDoubt }

func (b *FastBatch) Found(i int) bool { return b.results[i].found }

// RecordErr is nil for a found record, for NO_RESPONSE (as the official
// BatchRecord.Err) and for KEY_NOT_FOUND_ERROR on reads.
func (b *FastBatch) RecordErr(i int) Error {
	r := &b.results[i]
	switch {
	case r.found || r.code == types.NO_RESPONSE:
		return nil
	case r.code == types.KEY_NOT_FOUND_ERROR && b.entries[i].policy == nil:
		return nil
	}
	return newError(r.code).setNode(r.node).markInDoubtIf(r.inDoubt)
}

// Bin returns the first op result named name. Views stay valid until Reset.
func (b *FastBatch) Bin(i int, name string) (FastValue, bool) {
	ops := b.results[i].ops
	for len(ops) >= 8 {
		size := int(binary.BigEndian.Uint32(ops))
		nameLen := int(ops[7])
		if size < 4+nameLen || len(ops)-4 < size {
			break
		}
		if string(ops[8:8+nameLen]) == name {
			return FastValue{ptype: int(ops[5]), data: ops[8+nameLen : 4+size]}, true
		}
		ops = ops[4+size:]
	}
	return FastValue{}, false
}

const fastMsgpack = -1

// FastValue is a bin value (ptype is its particle type) or a msgpack element
// inside a map or list (ptype is fastMsgpack).
type FastValue struct {
	ptype int
	data  []byte
}

func (v FastValue) Int() (int64, bool) {
	p := v.data
	switch v.ptype {
	case particleType.INTEGER:
		switch len(p) {
		case 2:
			return int64(int16(binary.BigEndian.Uint16(p))), true
		case 4:
			return int64(int32(binary.BigEndian.Uint32(p))), true
		case 8:
			return int64(binary.BigEndian.Uint64(p)), true
		}
		if len(p) > 8 {
			return 0, false
		}
		var n int64
		for _, c := range p {
			n = n<<8 | int64(c)
		}
		return n, true
	case fastMsgpack:
		return mpInt(p)
	}
	return 0, false
}

func (v FastValue) Bytes() ([]byte, bool) {
	switch v.ptype {
	case particleType.STRING, particleType.BLOB:
		return v.data, true
	case fastMsgpack:
		kind, hdr, n, ok := mpHeader(v.data)
		if !ok || kind != mpBytes || n < 1 || len(v.data)-hdr < n {
			return nil, false
		}
		if t := v.data[hdr]; t != particleType.STRING && t != particleType.BLOB {
			return nil, false
		}
		return v.data[hdr+1 : hdr+n], true
	}
	return nil, false
}

func (v FastValue) Map() (FastMapIter, bool) {
	if v.ptype != particleType.MAP && v.ptype != fastMsgpack {
		return FastMapIter{}, false
	}
	kind, hdr, n, ok := mpHeader(v.data)
	if !ok || kind != mpMap {
		return FastMapIter{}, false
	}
	return FastMapIter{p: v.data[hdr:], left: n}, true
}

func (v FastValue) List() (FastListIter, bool) {
	if v.ptype != particleType.LIST && v.ptype != fastMsgpack {
		return FastListIter{}, false
	}
	kind, hdr, n, ok := mpHeader(v.data)
	if !ok || kind != mpArray {
		return FastListIter{}, false
	}
	it := FastListIter{p: v.data[hdr:], left: n}
	if n > 0 {
		if k, _, _, ok := mpHeader(it.p); ok && k == mpExt {
			if l := mpSkip(it.p, 0); l > 0 {
				it.p, it.left = it.p[l:], n-1
			}
		}
	}
	return it, true
}

var errFastMsgpack = newError(types.PARSE_ERROR, "FastBatch: malformed msgpack value")

type FastMapIter struct {
	p    []byte
	left int
	err  Error
}

// Next skips the ordered-map header entry.
func (it *FastMapIter) Next() (k, v FastValue, ok bool) {
	for it.left > 0 {
		it.left--
		kl := mpSkip(it.p, 0)
		if kl < 0 {
			it.err, it.left = errFastMsgpack, 0
			break
		}
		vl := mpSkip(it.p[kl:], 0)
		if vl < 0 {
			it.err, it.left = errFastMsgpack, 0
			break
		}
		key, val := it.p[:kl], it.p[kl:kl+vl]
		it.p = it.p[kl+vl:]
		if kind, _, _, _ := mpHeader(key); kind == mpExt {
			continue
		}
		return FastValue{ptype: fastMsgpack, data: key}, FastValue{ptype: fastMsgpack, data: val}, true
	}
	return FastValue{}, FastValue{}, false
}

func (it *FastMapIter) Err() error {
	if it.err == nil {
		return nil
	}
	return it.err
}

type FastListIter struct {
	p    []byte
	left int
	err  Error
}

func (it *FastListIter) Next() (v FastValue, ok bool) {
	if it.left <= 0 {
		return FastValue{}, false
	}
	it.left--
	l := mpSkip(it.p, 0)
	if l < 0 {
		it.err, it.left = errFastMsgpack, 0
		return FastValue{}, false
	}
	v = FastValue{ptype: fastMsgpack, data: it.p[:l]}
	it.p = it.p[l:]
	return v, true
}

func (it *FastListIter) Err() error {
	if it.err == nil {
		return nil
	}
	return it.err
}

const (
	mpFixed = iota
	mpBytes
	mpExt
	mpArray
	mpMap
)

const mpMaxDepth = 32

// mpHeader decodes the msgpack header at p[0]. For mpArray and mpMap n is the
// element or pair count; otherwise n is the payload length after hdr bytes.
func mpHeader(p []byte) (kind, hdr, n int, ok bool) {
	if len(p) == 0 {
		return 0, 0, 0, false
	}
	t := p[0]
	switch {
	case t <= 0x7f || t >= 0xe0 || t == 0xc0 || t == 0xc2 || t == 0xc3:
		return mpFixed, 1, 0, true
	case t&0xe0 == 0xa0:
		return mpBytes, 1, int(t & 0x1f), true
	case t&0xf0 == 0x80:
		return mpMap, 1, int(t & 0x0f), true
	case t&0xf0 == 0x90:
		return mpArray, 1, int(t & 0x0f), true
	}
	size := func(w int) (int, bool) {
		if len(p) < 1+w {
			return 0, false
		}
		switch w {
		case 1:
			return int(p[1]), true
		case 2:
			return int(binary.BigEndian.Uint16(p[1:])), true
		}
		return int(binary.BigEndian.Uint32(p[1:])), true
	}
	w := 0
	switch t {
	case 0xcc, 0xd0:
		return mpFixed, 1, 1, true
	case 0xcd, 0xd1:
		return mpFixed, 1, 2, true
	case 0xca, 0xce, 0xd2:
		return mpFixed, 1, 4, true
	case 0xcb, 0xcf, 0xd3:
		return mpFixed, 1, 8, true
	case 0xd4:
		return mpExt, 1, 2, true
	case 0xd5:
		return mpExt, 1, 3, true
	case 0xd6:
		return mpExt, 1, 5, true
	case 0xd7:
		return mpExt, 1, 9, true
	case 0xd8:
		return mpExt, 1, 17, true
	case 0xc4, 0xd9, 0xc7:
		w = 1
	case 0xc5, 0xda, 0xc8, 0xdc, 0xde:
		w = 2
	case 0xc6, 0xdb, 0xc9, 0xdd, 0xdf:
		w = 4
	default:
		return 0, 0, 0, false
	}
	n, ok = size(w)
	if !ok {
		return 0, 0, 0, false
	}
	switch t {
	case 0xc7, 0xc8, 0xc9:
		return mpExt, 1 + w, n + 1, true
	case 0xdc, 0xdd:
		return mpArray, 1 + w, n, true
	case 0xde, 0xdf:
		return mpMap, 1 + w, n, true
	}
	return mpBytes, 1 + w, n, true
}

// mpSkip returns the encoded length of the msgpack object at p[0], or -1 if it
// is malformed or runs past p.
func mpSkip(p []byte, depth int) int {
	kind, hdr, n, ok := mpHeader(p)
	if !ok {
		return -1
	}
	if kind != mpArray && kind != mpMap {
		if n > len(p)-hdr {
			return -1
		}
		return hdr + n
	}
	if kind == mpMap {
		n *= 2
	}
	if depth >= mpMaxDepth || n > len(p)-hdr {
		return -1
	}
	off := hdr
	for range n {
		l := mpSkip(p[off:], depth+1)
		if l < 0 {
			return -1
		}
		off += l
	}
	return off
}

func mpInt(p []byte) (int64, bool) {
	if len(p) == 0 {
		return 0, false
	}
	t := p[0]
	if t <= 0x7f {
		return int64(t), true
	}
	if t >= 0xe0 {
		return int64(int8(t)), true
	}
	_, hdr, n, ok := mpHeader(p)
	if !ok || len(p) < hdr+n {
		return 0, false
	}
	q := p[1:]
	switch t {
	case 0xcc:
		return int64(q[0]), true
	case 0xcd:
		return int64(binary.BigEndian.Uint16(q)), true
	case 0xce:
		return int64(binary.BigEndian.Uint32(q)), true
	case 0xcf, 0xd3:
		return int64(binary.BigEndian.Uint64(q)), true
	case 0xd0:
		return int64(int8(q[0])), true
	case 0xd1:
		return int64(int16(binary.BigEndian.Uint16(q))), true
	case 0xd2:
		return int64(int32(binary.BigEndian.Uint32(q))), true
	}
	return 0, false
}
