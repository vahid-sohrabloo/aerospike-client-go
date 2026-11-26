package aerospike

import (
	"fmt"

	"github.com/aerospike/aerospike-client-go/v8/types"
	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

type fastBatchCommand struct {
	batchCommand
	fb   *FastBatch
	resp []byte
}

// cmdFor returns a command slot kept in b. Slots are handed out in order and
// released by Reset.
func (b *FastBatch) cmdFor(node *Node) *fastBatchCommand {
	if b.nCmds == len(b.cmds) {
		b.cmds = append(b.cmds, &fastBatchCommand{batchCommand: batchCommand{batch: &batchNode{}}})
	}
	c := b.cmds[b.nCmds]
	b.nCmds++

	bn := c.batch
	*c = fastBatchCommand{fb: b, resp: c.resp[:0]}
	*bn = batchNode{Node: node, offsets: bn.offsets[:0]}
	c.batch = bn
	c.node = node
	c.isOperation = true
	return c
}

func (b *FastBatch) hasWrite() (bool, Error) {
	if len(b.entries) == 0 {
		return false, nil
	}
	write := b.entries[0].policy != nil
	for i := range b.entries {
		if (b.entries[i].policy != nil) != write {
			return false, newError(types.PARAMETER_ERROR, "FastBatch cannot mix reads and writes")
		}
	}
	return write, nil
}

// checkFast rejects what FastBatch does not encode, before any command is
// sent, so no record is left in doubt.
func (clnt *Client) checkFast(policy *BatchPolicy, b *FastBatch, write bool) Error {
	if policy.Txn != nil {
		return newError(types.PARAMETER_ERROR, "FastBatch does not support transactions")
	}
	if policy.FilterExpression != nil || !write && clnt.getUsableBatchReadPolicy(nil).FilterExpression != nil {
		return newError(types.PARAMETER_ERROR, "FastBatch does not support filter expressions")
	}
	for i := range b.entries {
		if p := b.entries[i].policy; p != nil && p.attr.filterExp != nil {
			return newError(types.PARAMETER_ERROR, "FastBatch does not support filter expressions")
		}
	}
	return nil
}

func (cmd *fastBatchCommand) writeBuffer(ifc command) Error {
	b := cmd.fb
	policy := cmd.policy
	write, err := b.hasWrite()
	if err != nil {
		return err
	}

	var readAttr batchAttr
	if !write {
		readAttr.setBatchRead(cmd.client.getUsableBatchReadPolicy(nil))
	}

	recordHeader := 4 + len(b.keys[0].digest) + 12 +
		len(b.namespace) + int(_FIELD_HEADER_SIZE) + len(b.set) + int(_FIELD_HEADER_SIZE)
	cmd.begin()
	cmd.dataOffset += int(_FIELD_HEADER_SIZE) + 5
	for _, i := range cmd.batch.offsets {
		e := &b.entries[i]
		cmd.dataOffset += recordHeader
		if write {
			cmd.dataOffset += 2 + e.opLen
		} else {
			cmd.dataOffset += len(e.bins.enc)
		}
	}

	if err := cmd.sizeBuffer(policy.compress()); err != nil {
		return err
	}

	cmd.writeBatchHeader(policy, 1)
	fieldSizeOffset := cmd.dataOffset
	cmd.writeFieldHeader(0, BATCH_INDEX)
	cmd.WriteUint32(uint32(len(cmd.batch.offsets)))
	cmd.WriteByte(cmd.getBatchFlags(policy))

	for _, i := range cmd.batch.offsets {
		e := &b.entries[i]
		key := &b.keys[i]
		cmd.WriteUint32(uint32(i))
		cmd.Write(key.digest[:])
		if write {
			cmd.writeBatchWrite(key, nil, nil, &e.policy.attr, nil, 0, e.opCount)
			cmd.Write(b.opBuf[e.opOff : e.opOff+e.opLen])
		} else {
			cmd.writeBatchRead(key, nil, nil, &readAttr, nil, e.bins.n)
			cmd.Write(e.bins.enc)
		}
	}

	cmd.WriteUint32At(uint32(cmd.dataOffset)-uint32(_MSG_TOTAL_HEADER_SIZE)-4, fieldSizeOffset)
	cmd.end()
	cmd.markCompressed(policy)
	return nil
}

func (cmd *fastBatchCommand) parseRecordResults(ifc command, receiveSize int) (bool, Error) {
	results := cmd.fb.results
	write := !cmd.isRead()
	cmd.dataOffset = 0
	for cmd.dataOffset < receiveSize {
		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			return false, err
		}
		resultCode := types.ResultCode(cmd.dataBuffer[5] & 0xFF)
		info3 := int(cmd.dataBuffer[3])
		lastMessage := info3&_INFO3_LAST == _INFO3_LAST
		if resultCode == 0 && lastMessage {
			return false, nil
		}

		batchIndex := int(Buffer.BytesToUint32(cmd.dataBuffer, 14))
		fieldCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 18))
		opCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 20))
		if batchIndex < 0 || batchIndex >= len(results) {
			return false, newError(types.PARSE_ERROR, fmt.Sprintf("FastBatch: batch index %d out of range", batchIndex)).setNode(cmd.node)
		}
		if err := cmd.skipKey(fieldCount); err != nil {
			return false, err
		}

		if (!write || resultCode == 0) && cmd.node.cluster.metricsEnabled.Load() {
			cmd.node.stats.updateOrInsert(cmd.getNamespace(), cmd.getNamespaces(), cmd.commandType(), resultCode)
		}

		r := &results[batchIndex]
		if resultCode != 0 {
			if resultCode == types.FILTERED_OUT {
				cmd.filteredOutCnt++
			}
			if lastMessage && receiveSize == int(_MSG_REMAINING_HEADER_SIZE) {
				return false, newError(resultCode).setNode(cmd.node)
			}
			r.code, r.node = resultCode, cmd.node
			r.inDoubt = cmd.batchInDoubt(write, cmd.commandSentCounter)
			if resultCode == types.UDF_BAD_RESPONSE {
				if err := cmd.readOps(opCount, r); err != nil {
					return false, err
				}
			}
			if lastMessage {
				return false, nil
			}
			continue
		}

		// A record cut inside its ops still counts as answered, so it is
		// neither in doubt nor retried, as in the official parsers.
		r.code, r.node = types.OK, cmd.node
		if err := cmd.readOps(opCount, r); err != nil {
			r.inDoubt = cmd.batchInDoubt(write, cmd.commandSentCounter)
			return false, err
		}
		r.inDoubt, r.found = false, true
	}
	return true, nil
}

// readOps copies the record's ops into cmd.resp. Commands that run in parallel
// each own their resp, so views are never shared across goroutines. Name and
// value are read separately, as the official parsers do, so a cut stream fails
// at the same record.
func (cmd *fastBatchCommand) readOps(opCount int, r *fastResult) Error {
	start := len(cmd.resp)
	for range opCount {
		if err := cmd.readBytes(8); err != nil {
			return err
		}
		opSize := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
		nameSize := int(cmd.dataBuffer[7])
		if opSize < 4+nameSize {
			return newError(types.PARSE_ERROR, fmt.Sprintf("FastBatch: op size %d too small for name size %d", opSize, nameSize)).setNode(cmd.node)
		}
		cmd.resp = append(cmd.resp, cmd.dataBuffer[:8]...)
		for _, n := range [2]int{nameSize, opSize - 4 - nameSize} {
			if err := cmd.readBytes(n); err != nil {
				return err
			}
			cmd.resp = append(cmd.resp, cmd.dataBuffer[:n]...)
		}
	}
	r.ops = cmd.resp[start:len(cmd.resp):len(cmd.resp)]
	return nil
}

func (cmd *fastBatchCommand) inDoubt() {
	if cmd.isRead() {
		return
	}
	for _, i := range cmd.batch.offsets {
		if r := &cmd.fb.results[i]; r.code == types.NO_RESPONSE {
			r.inDoubt = true
		}
	}
}

func (cmd *fastBatchCommand) isRead() bool {
	return len(cmd.fb.entries) == 0 || cmd.fb.entries[0].policy == nil
}

func (cmd *fastBatchCommand) commandType() commandType {
	if cmd.isRead() {
		return ttBatchRead
	}
	return ttBatchWrite
}

func (cmd *fastBatchCommand) getNamespace() *string {
	return &cmd.fb.namespace
}

func (cmd *fastBatchCommand) Execute() Error {
	err := cmd.execute(cmd)
	if err != nil {
		cmd.setInDoubt(cmd)
	}
	return err
}

func (cmd *fastBatchCommand) cloneBatchCommand(batch *batchNode) batcher {
	res := *cmd
	res.node = batch.Node
	res.batch = batch
	res.resp = nil
	return &res
}

func (cmd *fastBatchCommand) generateBatchNodes(cluster *Cluster) ([]*batchNode, Error) {
	if len(cluster.GetNodes()) == 0 {
		return nil, ErrClusterIsEmpty.err()
	}
	replicaPolicy := cmd.policy.ReplicaPolicy
	replicaPolicySC := GetReplicaPolicySC(cmd.policy.GetBasePolicy())
	write := !cmd.isRead()

	var batchNodes []*batchNode
	for _, offset := range cmd.batch.offsets {
		if write && cmd.fb.results[offset].code != types.NO_RESPONSE {
			continue
		}
		key := &cmd.fb.keys[offset]
		var node *Node
		var err Error
		if write {
			node, err = GetNodeBatchWrite(cluster, key, replicaPolicy, cmd.batch.Node, cmd.sequenceAP)
		} else {
			node, err = GetNodeBatchRead(cluster, key, replicaPolicy, replicaPolicySC, cmd.batch.Node, cmd.sequenceAP, cmd.sequenceSC)
		}
		if err != nil {
			return nil, err
		}
		if bn := findBatchNode(batchNodes, node); bn == nil {
			batchNodes = append(batchNodes, newBatchNode(node, len(cmd.batch.offsets), offset))
		} else {
			bn.AddKey(offset)
		}
	}
	return batchNodes, nil
}
