package aerospike

import (
	"testing"
)

var fastFuzzBinNames = []string{"rc", "bc", "bs", "la", "grc", "gla", "eids", "empty", "partner", "abtest", "n"}

func fastWalk(v FastValue, depth int) {
	v.Int()
	v.Bytes()
	if depth > 8 {
		return
	}
	if it, ok := v.Map(); ok {
		for {
			k, e, ok := it.Next()
			if !ok {
				break
			}
			fastWalk(k, depth+1)
			fastWalk(e, depth+1)
		}
		_ = it.Err()
	}
	if it, ok := v.List(); ok {
		for {
			e, ok := it.Next()
			if !ok {
				break
			}
			fastWalk(e, depth+1)
		}
		_ = it.Err()
	}
}

func fastFuzzBatches() []*FastBatch {
	r, _ := fastReadResultBatch()
	w := NewFastBatch("test", "sess")
	fwp := NewFastWritePolicy(fastTestWritePolicy())
	for i := range 4 {
		w.AddWriteBlob([]byte{byte(i)}, fwp).AddInt("n", 1)
	}
	return []*FastBatch{r, w}
}

func FuzzFastResultParse(f *testing.F) {
	f.Add(fastReadReply()[8:])
	f.Add(fastReplyMsg(
		fastReplyRec{index: 0, ops: []fastReplyOp{opInt("rc", 6), opInt("la", 3), opInt("la", 1), opNull("n")}},
		fastReplyRec{index: 1, code: 26},
		fastReplyLast(),
	)[8:])
	f.Add(fastReplyMsg(fastReplyRec{index: 0, ops: []fastReplyOp{
		opMap("bs", packIntMap(true, []int64{3}, [][]byte{packInts(1, 2, 3, 4)})),
	}}, fastReplyLast())[8:])

	clnt, policy, node := fastTestClient(), fastTestBatchPolicy(), fastTestNode()
	f.Fuzz(func(t *testing.T, body []byte) {
		reply := fastProtoMsg(body)
		for _, b := range fastFuzzBatches() {
			cmd, err := fastParse(b, clnt, policy, node, seqOffsets(b.Len()), 2, reply, 64)
			cmd.inDoubt()
			for i := range b.Len() {
				b.ResultCode(i)
				b.InDoubt(i)
				b.Found(i)
				_ = b.RecordErr(i)
				if err != nil {
					continue
				}
				for _, name := range fastFuzzBinNames {
					if v, ok := b.Bin(i, name); ok {
						fastWalk(v, 0)
					}
				}
			}
		}
	})
}
