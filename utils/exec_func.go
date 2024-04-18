package utils

import (
	"context"
	"encoding/csv"
	"erigonInteract/gria"
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

	// txs, predictRwSets, _ := GetTxsAndPredicts(blockReader, ctx, dbTx, blockNum)
	txs, predictRwSets, rwAccessedBy := GetTxsAndPredicts(blockReader, ctx, dbTx, blockNum)
	trueRwSets, err := TrueRWSets(blockReader, ctx, dbTx, blockNum)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}
	// for i, tx := range txs {
	// 	if tx.GetTo().String() == "0xdAC17F958D2ee523a2206206994597C13D831ec7" {
	// 		fmt.Println("Tx:", i)
	// 		fmt.Println("PredictRWSet:", predictRwSets[i].ToJsonStruct().ToString())
	// 	}
	// }
	// 用预测的和真实的rwsets来预取数据构建并发statedb
	scatterState := interactState.NewScatterState()
	scatterState.Prefetch(ibs, predictRwSets)
	scatterState.Prefetch(ibs, trueRwSets)
	fmt.Println("----------------------------------------")
	// 准备线程池
	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	// antsWG.Add(2)
	// antsPool.Submit(func() {
	// 	tracer.ExecuteTxs(blkCtx, types.Transactions{txs[33]}, header, ibs)
	// 	antsWG.Done()
	// })
	// antsPool.Submit(func() {
	// 	tracer.ExecuteTxs(blkCtx, types.Transactions{txs[15]}, header, ibs)
	// 	antsWG.Done()
	// })
	// antsWG.Wait()
	// return 0, 0, 0, 0, 0, 0, nil

	// 建图
	// 建图分组
	graphStart := time.Now()
	// 使用预取RWset构建图
	vIdsGroups := GenerateVertexIdGroups(txs, rwAccessedBy)
	graphTime := time.Since(graphStart)

	groupstart := time.Now()
	// 似乎已经不需要Rwsetgroup了，因为不需要再通过分组进行预取了
	txGroup, _ := GenerateCCGroups(vIdsGroups, txs, predictRwSets)
	// txGroup, RwSetGroup := utils.GenerateTxAndRWSetGroups(txs, predictRWSet)
	groupTime := time.Since(groupstart)
	createGraphTime := time.Since(graphStart)

	// 并发执行
	execStart := time.Now()
	tracer.ExecConflictedTxs(antsPool, txGroup, scatterState, header, blkCtx, &antsWG)
	execTime := time.Since(execStart)

	// 总时间
	timeSum := time.Since(graphStart)

	// 返回建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	// return len(txs), int64(graphTime.Microseconds()), int64(groupTime.Microseconds()), int64(createGraphTime.Microseconds()), int64(prefectTime.Microseconds()), int64(execTime.Microseconds()), int64(mergeTime.Microseconds()), int64(timeSum.Microseconds()), nil
	return len(txs), int64(graphTime.Microseconds()), int64(groupTime.Microseconds()), int64(createGraphTime.Microseconds()), int64(execTime.Microseconds()), int64(timeSum.Microseconds()), nil
}

func MISTest(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) error {
	misfile, err := os.Create(("mis.csv"))
	if err != nil {
		panic(err)
	}
	defer misfile.Close()
	misWriter := csv.NewWriter(misfile)
	defer misWriter.Flush()
	// 建图时间，分组时间，建图分组总时间，执行时间，总时间
	err = misWriter.Write([]string{"BlockNum", "TxNum", "graph", "group", "graph+group", "execute", "total"})
	if err != nil {
		panic(err)
	}

	fmt.Println("test start")
	for i := 0; i < 10; i++ {
		blockNum := blockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		txsNum, graphTime, groupTime, graphGroupTime, executeTime, totalTime, _ := MISExec(blockReader, ctx, dbTx, blockNum)
		err = misWriter.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(txsNum), fmt.Sprint(graphTime), fmt.Sprint(groupTime), fmt.Sprint(graphGroupTime), fmt.Sprint(executeTime), fmt.Sprint(totalTime)})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func MISExec(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) (int, int64, int64, int64, int64, int64, error) {
	fmt.Println("MIS Execution")
	block, header := GetBlockAndHeader(blockReader, ctx, dbTx, blockNum)
	blkCtx := GetBlockContext(blockReader, block, dbTx, header)
	ibs := GetState(params.MainnetChainConfig, dbTx, blockNum)

	txs, predictRwSets, rwAccessedBy := GetTxsAndPredicts(blockReader, ctx, dbTx, blockNum)
	trueRwSets, err := TrueRWSets(blockReader, ctx, dbTx, blockNum)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	// 用预测的和真实的rwsets来预取数据构建并发statedb
	scatterState := interactState.NewScatterState()
	scatterState.Prefetch(ibs, predictRwSets)
	scatterState.Prefetch(ibs, trueRwSets)
	fmt.Println("----------------------------------------")
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()
	var antsWG sync.WaitGroup

	// st := time.Now()
	// groups := utils.GenerateMISGroups(txs, predictRwSets)
	// fmt.Println("Generate TxGroups:", time.Since(st))
	// 建图
	graphStart := time.Now()
	graph := GenerateUndiGraph(len(txs), rwAccessedBy)
	graphTime := time.Since(graphStart)
	fmt.Println("graphtime:", graphTime)

	// 分组
	groupstart := time.Now()
	groups := SolveMISInTurn(graph)
	groupTime := time.Since(groupstart)

	createGraphTime := time.Since(graphStart)
	fmt.Println("grouptime:", groupTime)

	PureExecutionCost := time.Duration(0)

	for round := 0; round < len(groups); round++ {
		txsToExec := GenerateTxToExec(groups[round], txs)
		execst := time.Now()
		tracer.ExecConflictFreeTxs(antsPool, txsToExec, scatterState, header, blkCtx, &antsWG)
		PureExecutionCost += time.Since(execst)
		// fmt.Println("exec time:", time.Since(execst))
	}

	// execTime := time.Since(st)
	// 总时间
	timeSum := time.Since(graphStart)

	// 返回建图时间，分组时间，建图分组总时间，执行时间，多轮时间，总时间
	return len(txs), int64(graphTime.Microseconds()), int64(groupTime.Microseconds()), int64(createGraphTime.Microseconds()), int64(PureExecutionCost.Microseconds()), int64(timeSum.Microseconds()), nil
}

func DAGTest(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) error {
	dagfile, err := os.Create(("dag.csv"))
	if err != nil {
		panic(err)
	}
	defer dagfile.Close()
	dagWriter := csv.NewWriter(dagfile)
	defer dagWriter.Flush()
	// 建图时间，分组时间，建图分组总时间, 执行时间，总时间
	err = dagWriter.Write([]string{"BlockNum", "TxNum", "graph", "group", "graph+group", "execute", "total"})
	if err != nil {
		panic(err)
	}

	fmt.Println("test start")
	for i := 0; i < 10; i++ {
		blockNum := blockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		// testfunc.CCTest1(txs, predictRWSet, header, fakeChainCtx, state)
		txsNum, graphTime, groupTime, graphGroupTime, executeTime, totalTime, _ := DAGExec(blockReader, ctx, dbTx, blockNum)
		err = dagWriter.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(txsNum), fmt.Sprint(graphTime), fmt.Sprint(groupTime), fmt.Sprint(graphGroupTime), fmt.Sprint(executeTime), fmt.Sprint(totalTime)})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func DAGExec(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) (int, int64, int64, int64, int64, int64, error) {
	fmt.Println("DegreeZero Solution  Execution")
	block, header := GetBlockAndHeader(blockReader, ctx, dbTx, blockNum)
	blkCtx := GetBlockContext(blockReader, block, dbTx, header)
	ibs := GetState(params.MainnetChainConfig, dbTx, blockNum)

	// txs, predictRwSets, _ := GetTxsAndPredicts(blockReader, ctx, dbTx, blockNum)
	txs, predictRwSets, rwAccessedBy := GetTxsAndPredicts(blockReader, ctx, dbTx, blockNum)
	trueRwSets, err := TrueRWSets(blockReader, ctx, dbTx, blockNum)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	// 用预测的和真实的rwsets来预取数据构建并发statedb
	scatterState := interactState.NewScatterState()
	scatterState.Prefetch(ibs, predictRwSets)
	scatterState.Prefetch(ibs, trueRwSets)
	scatterState.Equal(ibs, predictRwSets)
	fmt.Println("----------------------------------------")

	// !! 这一串注释用于执行单个交易
	// scatterEvm := vm.NewEVM(blkCtx, evmtypes.TxContext{}, scatterState, params.MainnetChainConfig, vm.Config{})
	// res, _ := tracer.ExecuteTx(scatterState, txs[5], header, scatterEvm)
	// if res.Err != nil {
	// 	fmt.Println("Error executing transaction in VM layer:", res.Err)
	// }

	// !! 这一串注释用于使用ScatterState串行执行
	// st := time.Now()
	// tracer.ExecuteTxs(blkCtx, txs, header, scatterState)
	// fmt.Println("Serial Execution with scatterState Time:", time.Since(st))
	// return 0, 0, 0, 0, 0, 0, nil

	// !! 这一串是正牌DAG
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()
	var antsWG sync.WaitGroup

	// 建图
	graphStart := time.Now()
	graph := GenerateDiGraph(len(txs), rwAccessedBy)
	graphTime := time.Since(graphStart)
	fmt.Println("graphtime:", graphTime)

	// 分组
	groupstart := time.Now()
	groups := graph.GetTopo()
	groupTime := time.Since(groupstart)
	createGraphTime := time.Since(graphStart)
	fmt.Println("grouptime:", groupTime)

	PureExecutionCost := time.Duration(0)

	for round := 0; round < len(groups); round++ {
		txsToExec := GenerateTxToExec(groups[round], txs)
		execst := time.Now()
		tracer.ExecConflictFreeTxs(antsPool, txsToExec, scatterState, header, blkCtx, &antsWG)
		PureExecutionCost += time.Since(execst)
	}
	fmt.Println("exec time:", PureExecutionCost)
	// 总时间
	timeSum := time.Since(graphStart)

	// 返回建图时间，分组时间，建图分组总时间，执行时间，多轮时间，总时间
	return len(txs), int64(graphTime.Microseconds()), int64(groupTime.Microseconds()), int64(createGraphTime.Microseconds()), int64(PureExecutionCost.Microseconds()), int64(timeSum.Microseconds()), nil
}

func GriaExec(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64, workerNum int) {
	fmt.Println("Gria Execution")
	block, header := GetBlockAndHeader(blockReader, ctx, dbTx, blockNum)
	blkCtx := GetBlockContext(blockReader, block, dbTx, header)
	ibs := GetState(params.MainnetChainConfig, dbTx, blockNum)

	txs, predictRwSets, _ := GetTxsAndPredicts(blockReader, ctx, dbTx, blockNum)
	trueRwSets, err := TrueRWSets(blockReader, ctx, dbTx, blockNum)
	if err != nil {
		// return 0, 0, 0, 0, 0, 0, err
		return
	}

	// 用预测的和真实的rwsets来预取数据构建并发statedb
	scatterState := interactState.NewScatterState()
	scatterState.Prefetch(ibs, predictRwSets)
	scatterState.Prefetch(ibs, trueRwSets)
	fmt.Println("----------------------------------------")
	// 初始化全局版本链
	gvc := interactState.NewGlobalVersionChain()

	st := time.Now()
	// 为每个Processor制作状态代理
	states := make([]*interactState.StateForGria, workerNum)
	for i := 0; i < workerNum; i++ {
		states[i] = interactState.NewStateForGria(scatterState, gvc)
	}

	// 贪心分组
	txGroups := gria.GreedyGrouping(txs, workerNum)

	// 制作Processor
	GriaProcessor := make([]*GriaGroupWrapper, workerNum)
	for i := 0; i < workerNum; i++ {
		GriaProcessor[i] = NewGriaGroupWrapper(states[i], txGroups[i], header, blkCtx)
	}

	// 执行Tx
	wg := sync.WaitGroup{}
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go GriaProcessor[i].ProcessTxs(&wg)
	}
	wg.Wait()

	// 提交Tx
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go GriaProcessor[i].CommitTxs(&wg)
	}
	wg.Wait()
	sum := 0
	for i := 0; i < workerNum; i++ {
		sum += GriaProcessor[i].GetAbortNum()
	}
	fmt.Println("Aborted before rechecking:", sum)

	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go GriaProcessor[i].RecheckTxs(&wg)
	}
	wg.Wait()
	sum = 0
	for i := 0; i < workerNum; i++ {
		sum += GriaProcessor[i].GetAbortNum()
	}
	fmt.Println("Aborted after rechecking:", sum)

	fmt.Println("Gria Execution Time:", time.Since(st))
}
