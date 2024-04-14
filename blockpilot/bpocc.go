package blockpilot

import (
	"context"
	"erigonInteract/accesslist"
	"fmt"

	interactState "erigonInteract/state"
	"erigonInteract/tracer"
	"erigonInteract/utils"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"

	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"

	"github.com/golang-collections/collections/stack"
	"github.com/panjf2000/ants/v2"
)

var BPMUTEX sync.Mutex
var Table map[common.Hash]int
var txStack = stack.New()

func BlockPilot(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) error {
	blk, header := utils.GetBlockAndHeader(blockReader, ctx, dbTx, blockNum)
	// 获取Tx和初始的stateDB
	txs, _, _ := utils.GetTxsAndPredicts(blockReader, ctx, dbTx, blockNum)

	state := utils.GetState(params.MainnetChainConfig, dbTx, blockNum)

	// 将txs转化成堆栈
	for _, tx := range txs {
		txStack.Push(tx)
	}

	blkCtx := utils.GetBlockContext(blockReader, blk, dbTx, header)
	// 初始化一个全局表Table = map[key]version
	Table = make(map[common.Hash]int)

	// 准备线程池
	var wg sync.WaitGroup
	pool, err := ants.NewPool(6)
	if err != nil {
		fmt.Printf("Failed to create pool: %v\n", err)
		return err
	}

	// 循环执行交易
	for i := 0; i < txs.Len(); i++ {
		wg.Add(1)

		// 提交任务到线程池
		err := pool.Submit(func() {
			// tx <- popHead
			tx := txStack.Pop().(types.Transaction)
			// snapshot
			version := state.Snapshot()

			// TODO：反正也不用合并
			cacheState := state

			// rs,ws <- Execute
			rws, err := ExecuteTx(tx, cacheState, header, blkCtx)
			if err != nil {
				panic(err)
			}

			// DetectConflict
			DetectConflict(tx, rws, version)

			wg.Done()
		})

		if err != nil {
			fmt.Printf("Failed to submit task: %v\n", err)
			break
		}
	}

	return nil
}

func ExecuteTx(tx types.Transaction, ibs *state.IntraBlockState, header *types.Header, blkCtx evmtypes.BlockContext) (*accesslist.RWSet, error) {
	fulldb := interactState.NewStateWithRwSets(ibs)
	rws, err := tracer.ExecToGenerateRWSet(fulldb, tx, header, blkCtx)
	if err != nil {
		return nil, err
	}
	return rws, nil
}

func DetectConflict(tx types.Transaction, rws *accesslist.RWSet, snapshotVersion int) bool {
	// 检查键是否存在
	for _, sValue := range rws.ReadSet {
		BPMUTEX.Lock()
		for key, _ := range sValue {
			if version, ok := Table[key]; ok {
				// 若key存在，则比较version
				if version > snapshotVersion {
					txStack.Push(tx)
					return false
				}
			} else {
				Table[key] = snapshotVersion
			}
		}
	}

	return true
}
