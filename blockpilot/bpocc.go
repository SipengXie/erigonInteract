package blockpilot

import (
	"context"
	"erigonInteract/accesslist"
	"fmt"
	"time"

	interactState "erigonInteract/state"
	"erigonInteract/tracer"
	"erigonInteract/utils"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
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

func BlockPilot(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) (int64, int64, error) {
	blk, header := utils.GetBlockAndHeader(blockReader, ctx, dbTx, blockNum)
	// 获取Tx和初始的stateDB
	txs, predictRwSets, _ := utils.GetTxsAndPredicts(blockReader, ctx, dbTx, blockNum)

	ibs := utils.GetState(params.MainnetChainConfig, dbTx, blockNum)
	trueRwSets, _ := utils.TrueRWSets(blockReader, ctx, dbTx, blockNum)
	scatterState := interactState.NewScatterState()
	scatterState.Prefetch(ibs, predictRwSets)
	scatterState.Prefetch(ibs, trueRwSets)
	// 将txs转化成堆栈
	for _, tx := range txs {
		txStack.Push(tx)
	}

	blkCtx := utils.GetBlockContext(blockReader, blk, dbTx, header)
	// 初始化一个全局表Table = map[key]version
	Table = make(map[common.Hash]int)

	// 准备线程池
	var wg sync.WaitGroup

	// TODO：这里的线程池大小是12，可以根据实际情况调整
	pool, err := ants.NewPool(12)
	if err != nil {
		fmt.Printf("Failed to create pool: %v\n", err)
		return 0, 0, err
	}

	// 准备用于复制的stateDB
	cachestate := interactState.NewScatterState()
	cachestate.Prefetch(ibs, predictRwSets)
	cachestate.Prefetch(ibs, trueRwSets)

	start := time.Now()
	// 循环执行交易
	errs := make([]error, 0)

	for txStack.Len() > 0 {

		wg.Add(1)
		// 提交任务到线程池
		err := pool.Submit(func() {
			// tx <- popHead
			if txStack.Len() == 0 {
				return
			}
			tx := txStack.Pop().(types.Transaction)
			// snapshot
			version := ibs.Snapshot()

			cacheState := interactState.CopyScatterState(cachestate)

			// rs,ws <- Execute
			rws, err := ExecuteTx(tx, cacheState, header, blkCtx)
			errs = append(errs, err)
			// if err != nil {
			// 	fmt.Printf("Failed to execute tx: %v\n", err)
			// }

			// DetectConflict
			BPMUTEX.Lock()
			DetectConflict(tx, rws, version)
			BPMUTEX.Unlock()

			wg.Done()
		})

		if err != nil {
			fmt.Printf("Failed to submit task: %v\n", err)
			break
		}
	}

	// fmt.Println("blockNum:", blockNum, "BlockPilot Time:", time.Since(start))
	exectime := time.Since(start)

	return int64(txs.Len()), exectime.Milliseconds(), nil
}

func ExecuteTx(tx types.Transaction, sdb *interactState.ScatterState, header *types.Header, blkCtx evmtypes.BlockContext) (*accesslist.RWSet, error) {
	rws, _, err := tracer.ExecToGenerateRWSet2(sdb, tx, header, blkCtx)
	if err != nil {
		return nil, err
	}
	return rws, nil
}

func DetectConflict(tx types.Transaction, rws *accesslist.RWSet, snapshotVersion int) bool {
	if rws == nil {
		return true
	}
	// 检查键是否存在
	for _, sValue := range rws.ReadSet {
		for key := range sValue {
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
