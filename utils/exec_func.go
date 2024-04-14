package utils

import (
	"context"
	"encoding/csv"
	interactState "erigonInteract/state"
	"erigonInteract/tracer"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/panjf2000/ants/v2"
)

// for Apex test
func SerialTest(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, startBlockNum uint64) {
	serialtestfile, err := os.Create(("serial.csv"))
	if err != nil {
		panic(err)
	}
	defer serialtestfile.Close()
	serialWriter := csv.NewWriter(serialtestfile)
	defer serialWriter.Flush()
	err = serialWriter.Write([]string{"BlockNum", "SerialTime", "TransactionNum"})
	if err != nil {
		panic(err)
	}

	// serial execution
	for i := 0; i < 10000; i++ {
		blockNum := startBlockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		serialTime, txsNum, _ := SerialExec(blockReader, ctx, dbTx, blockNum)
		err = serialWriter.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(serialTime), fmt.Sprint(txsNum)})
		if err != nil {
			panic(err)
		}
	}
}

// 串行执行
func SerialExec(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) (int64, int, error) {
	fmt.Println("Serial Execution")
	// get block and header
	blk, header := GetBlockAndHeader(blockReader, ctx, dbTx, blockNum)
	txs := blk.Transactions()
	// get statedb
	ibs := GetState(params.MainnetChainConfig, dbTx, blockNum)
	// get block context
	blkCtx := GetBlockContext(blockReader, blk, dbTx, header)
	// set the satrt time
	start := time.Now()
	// test the serial execution
	tracer.ExecuteTxs(blkCtx, txs, header, ibs)
	// cal the execution time
	elapsed := time.Since(start)
	fmt.Println("Serial Execution Time:", elapsed)

	return int64(elapsed.Microseconds()), len(txs), nil
}

// for Apex test
func CCTest(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) error {
	fmt.Println("ConnectedComponent Execution")

	cc1file, err := os.Create(("cc1.csv"))
	if err != nil {
		panic(err)
	}
	defer cc1file.Close()
	cc1Writer := csv.NewWriter(cc1file)
	defer cc1Writer.Flush()
	// 建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	err = cc1Writer.Write([]string{"BlockNum", "TxNum", "graph", "group", "graph+group", "execute", "total"})
	if err != nil {
		panic(err)
	}

	fmt.Println("test start")
	for i := 0; i < 10; i++ {
		// txs, predictRWSet, header, fakeChainCtx := GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)
		blockNum := blockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		// testfunc.CCTest1(txs, predictRWSet, header, fakeChainCtx, state)
		txNum, graphTime, groupTime, graphGroupTime, executeTime, totalTime, _ := CCExec(blockReader, ctx, dbTx, blockNum)
		err = cc1Writer.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(txNum), fmt.Sprint(graphTime), fmt.Sprint(groupTime), fmt.Sprint(graphGroupTime), fmt.Sprint(executeTime), fmt.Sprint(totalTime)})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

// 连通分量执行
func CCExec(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) (int, int64, int64, int64, int64, int64, error) {
	fmt.Println("ConnectedComponent Execution")
	block, header := GetBlockAndHeader(blockReader, ctx, dbTx, blockNum)
	blkCtx := GetBlockContext(blockReader, block, dbTx, header)
	ibs := GetState(params.MainnetChainConfig, dbTx, blockNum)

	txs, predictRwSets := GetTxsAndPredicts(blockReader, ctx, dbTx, blockNum)
	trueRwSets, err := TrueRWSets(blockReader, ctx, dbTx, blockNum)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}
	// 用预测的和真实的rwsets来预取数据构建并发statedb
	scatterState := interactState.NewScatterState()
	// scatterState.Prefetch(ibs, predictRwSets)
	scatterState.Prefetch(ibs, trueRwSets)

	// 准备线程池
	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	// 建图
	// 建图分组
	graphStart := time.Now()
	// 使用预取RWset构建图
	graph := GenerateVertexGroups(txs, predictRwSets)
	graphTime := time.Since(graphStart)

	groupstart := time.Now()
	// 似乎已经不需要Rwsetgroup了，因为不需要再通过分组进行预取了
	txGroup, _ := GenerateCCGroups(graph, txs, predictRwSets)
	// txGroup, RwSetGroup := utils.GenerateTxAndRWSetGroups(txs, predictRWSet)
	groupTime := time.Since(groupstart)
	createGraphTime := time.Since(graphStart)

	// // 准备状态数据库
	// state, err := utils.GetState(chainDB, sdbBackend, blockNum-1)
	// if err != nil {
	// 	return 0, 0, 0, 0, 0, 0, 0, 0, err
	// }
	// fullcache := interactState.NewFullCacheConcurrent()
	// // here we don't pre warm the data
	// fullcache.Prefetch(state, predictRwSets)

	// // 并发预取
	// prefectStart := time.Now()
	// cacheStates := utils.GenerateCacheStatesConcurrent(antsPool, fullcache, RwSetGroup, &antsWG)
	// prefectTime := time.Since(prefectStart)

	// 并发执行
	execStart := time.Now()
	tracer.ExecConflictedTxs(antsPool, txGroup, scatterState, header, blkCtx, &antsWG)
	execTime := time.Since(execStart)

	// // 并发合并
	// mergeStart := time.Now()
	// utils.MergeToCacheStateConcurrent(antsPool, cacheStates, fullcache, &antsWG)
	// mergeTime := time.Since(mergeStart)

	// 总时间
	timeSum := time.Since(graphStart)

	// 返回建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	// return len(txs), int64(graphTime.Microseconds()), int64(groupTime.Microseconds()), int64(createGraphTime.Microseconds()), int64(prefectTime.Microseconds()), int64(execTime.Microseconds()), int64(mergeTime.Microseconds()), int64(timeSum.Microseconds()), nil
	return len(txs), int64(graphTime.Microseconds()), int64(groupTime.Microseconds()), int64(createGraphTime.Microseconds()), int64(execTime.Microseconds()), int64(timeSum.Microseconds()), nil
}
