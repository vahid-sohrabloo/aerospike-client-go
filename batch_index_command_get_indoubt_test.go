// Copyright 2014-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"testing"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// A split retry of a batch read calls setInDoubt on every subcommand.
// batchIndexCommandGet holds its records in its own field, so the embedded
// batchCommandOperate.records is nil and its promoted inDoubt would index that
// nil slice with this subcommand's offsets.
func TestBatchIndexCommandGetInDoubt(t *testing.T) {
	records := make([]*BatchRead, 3)
	for i := range records {
		key, err := NewKey("test", "set", i)
		if err != nil {
			t.Fatal(err)
		}
		records[i] = NewBatchRead(nil, key, []string{"bin"})
	}

	batch := newBatchNode(nil, 10, 1)
	batch.AddKey(2)

	cmd := newBatchIndexCommandGet(nil, batch, NewBatchPolicy(), records, true)
	cmd.setInDoubt(&cmd)

	for i, br := range records {
		if br.InDoubt {
			t.Errorf("record %d: read marked in-doubt", i)
		}
		if br.ResultCode != types.NO_RESPONSE {
			t.Errorf("record %d: result code = %v, want NO_RESPONSE", i, br.ResultCode)
		}
	}
}
