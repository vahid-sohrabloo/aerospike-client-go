package aerospike

import (
	"bytes"
	"fmt"
	"math/rand/v2"

	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

type ExpTemplate struct {
	segs    [][]byte
	slots   []int
	nParams int
}

// ExpParam is an int placeholder that packs only inside CompileExpTemplate.
func ExpParam(i int) *Expression {
	return newFilterExpression(nil, expParamValue(i), nil, nil, nil, nil)
}

func CompileExpTemplate(e *Expression, nParams int) (*ExpTemplate, error) {
	packed, slots, err := expTemplatePack(e, nParams, nil)
	if err != nil {
		return nil, err
	}
	t := &ExpTemplate{slots: slots, nParams: nParams}
	used := make([]bool, nParams)
	rest := packed
	for site, p := range slots {
		used[p] = true
		s := appendPackInt(nil, expSentinel(site))
		if n := bytes.Count(packed, s); n != 1 {
			return nil, newError(types.PARAMETER_ERROR, fmt.Sprintf("exp template: sentinel of param site %d found %d times", site, n))
		}
		j := bytes.Index(rest, s)
		if j < 0 {
			return nil, newError(types.PARAMETER_ERROR, fmt.Sprintf("exp template: param site %d out of order", site))
		}
		t.segs = append(t.segs, rest[:j])
		rest = rest[j+len(s):]
	}
	t.segs = append(t.segs, rest)
	for i, u := range used {
		if !u {
			return nil, newError(types.PARAMETER_ERROR, fmt.Sprintf("exp template: param %d is not used", i))
		}
	}

	for _, gen := range []func() int64{
		func() int64 { return int64(rand.Uint64()) },
		func() int64 { return rand.Int64N(1 << 16) },
	} {
		params := make([]int64, nParams)
		for i := range params {
			params[i] = gen()
		}
		want, _, err := expTemplatePack(e, nParams, params)
		if err != nil {
			return nil, err
		}
		if got := t.appendExp(nil, params); !bytes.Equal(want, got) {
			return nil, newError(types.PARAMETER_ERROR, fmt.Sprintf("exp template: rebuilt bytes differ for params %v", params))
		}
	}
	return t, nil
}

func (w *FastWrite) ExpWrite(bin string, t *ExpTemplate, flags ExpWriteFlags, params []int64) {
	if len(params) != t.nParams {
		panic(newError(types.PARAMETER_ERROR, fmt.Sprintf("ExpWrite: got %d params, template takes %d", len(params), t.nParams)))
	}
	start := w.beginOp(_EXP_MODIFY, particleType.BLOB, bin)
	buf := append(w.b.opBuf, 0x92)
	buf = t.appendExp(buf, params)
	buf = appendPackInt(buf, int64(flags))
	w.endOp(buf, start)
}

func (t *ExpTemplate) appendExp(buf []byte, params []int64) []byte {
	buf = append(buf, t.segs[0]...)
	for i, p := range t.slots {
		buf = appendPackInt(buf, params[p])
		buf = append(buf, t.segs[i+1]...)
	}
	return buf
}

// expSentinel values pack as 9 bytes, the msgpack maximum, so size() of an
// expression with params is an upper bound for any binding.
func expSentinel(site int) int64 {
	return 0x5a3c9e71_00000000 + int64(site)
}

type expTemplateBuffer struct {
	*bufferEx
	nParams int
	params  []int64
	slots   []int
}

// expTemplatePack packs e with each ExpParam site set to params[i], or to a
// per-site sentinel when params is nil. It returns the param index per site.
func expTemplatePack(e *Expression, nParams int, params []int64) ([]byte, []int, Error) {
	sz, err := e.size()
	if err != nil {
		return nil, nil, err
	}
	buf := &expTemplateBuffer{bufferEx: newBuffer(sz), nParams: nParams, params: params}
	if _, err := e.pack(buf); err != nil {
		return nil, nil, err
	}
	return buf.Bytes(), buf.slots, nil
}

type expParamValue int

func (v expParamValue) pack(cmd BufferEx) (int, Error) {
	if cmd == nil {
		return 9, nil
	}
	buf, ok := cmd.(*expTemplateBuffer)
	if !ok {
		return 0, newError(types.PARAMETER_ERROR, "ExpParam is only valid inside CompileExpTemplate")
	}
	if int(v) < 0 || int(v) >= buf.nParams {
		return 0, newError(types.PARAMETER_ERROR, fmt.Sprintf("ExpParam(%d) out of range for %d params", int(v), buf.nParams))
	}
	val := expSentinel(len(buf.slots))
	if buf.params != nil {
		val = buf.params[v]
	}
	buf.slots = append(buf.slots, int(v))
	return IntegerValue(val).pack(cmd)
}

func (v expParamValue) EstimateSize() (int, Error) {
	return 0, newError(types.PARAMETER_ERROR, "ExpParam is only valid inside CompileExpTemplate")
}

func (v expParamValue) write(BufferEx) (int, Error) {
	return 0, newError(types.PARAMETER_ERROR, "ExpParam is only valid inside CompileExpTemplate")
}

func (v expParamValue) GetType() int { return particleType.INTEGER }

func (v expParamValue) GetObject() any { return int(v) }

func (v expParamValue) String() string { return fmt.Sprintf("ExpParam(%d)", int(v)) }
