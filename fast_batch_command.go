package aerospike

import (
	"github.com/aerospike/aerospike-client-go/v8/types"
)

type fastBatchCommand struct {
	batchCommand
	fb      *FastBatch
	offsets []int
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
	*c = fastBatchCommand{fb: b, offsets: c.offsets[:0]}
	*bn = batchNode{Node: node}
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

func (cmd *fastBatchCommand) writeBuffer(ifc command) Error {
	b := cmd.fb
	policy := cmd.policy
	if policy.Txn != nil || policy.FilterExpression != nil {
		return newError(types.PARAMETER_ERROR, "FastBatch does not support transactions or filter expressions")
	}
	write, err := b.hasWrite()
	if err != nil {
		return err
	}

	var readAttr batchAttr
	if !write {
		rp := cmd.client.getUsableBatchReadPolicy(nil)
		if rp.FilterExpression != nil {
			return newError(types.PARAMETER_ERROR, "FastBatch does not support filter expressions")
		}
		readAttr.setBatchRead(rp)
	}

	recordHeader := 4 + len(b.keys[0].digest) + 12 +
		len(b.namespace) + int(_FIELD_HEADER_SIZE) + len(b.set) + int(_FIELD_HEADER_SIZE)
	cmd.begin()
	cmd.dataOffset += int(_FIELD_HEADER_SIZE) + 5
	for _, i := range cmd.offsets {
		e := &b.entries[i]
		cmd.dataOffset += recordHeader
		if write {
			if e.policy.attr.filterExp != nil {
				return newError(types.PARAMETER_ERROR, "FastBatch does not support filter expressions")
			}
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
	cmd.WriteUint32(uint32(len(cmd.offsets)))
	cmd.WriteByte(cmd.getBatchFlags(policy))

	for _, i := range cmd.offsets {
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
	return false, newError(types.PARSE_ERROR, "FastBatch reply parsing is not implemented")
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
	res.offsets = batch.offsets
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
	for _, offset := range cmd.offsets {
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
			batchNodes = append(batchNodes, newBatchNode(node, len(cmd.offsets), offset))
		} else {
			bn.AddKey(offset)
		}
	}
	return batchNodes, nil
}
