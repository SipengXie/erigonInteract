package main

import (
	"context"
	"erigonInteract/accesslist"
	"erigonInteract/gria"
	"erigonInteract/schedule"
	interactState "erigonInteract/state"
	"erigonInteract/tracer"
	"erigonInteract/utils"
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/panjf2000/ants/v2"
)

func ApesPtest(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64, workerNum int) {
	for i := 0; i < 500; i = i + 10 {
		fmt.Println("ApesPtest,block from ", blockNum+uint64(i), " to ", blockNum+uint64(i+9))
		apexPlusExec(blockReader, ctx, dbTx, blockNum+uint64(i), workerNum)
	}
}

func apexPlusExec(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64, workerNum int) {
	k := 10
	blocks := make([]*types.Block, k)
	headers := make([]*types.Header, k)

	ibss := make([]evmtypes.IntraBlockState, k)
	txss := make([]types.Transaction, 0)
	predictRwSetss := make([]*accesslist.RWSet, 0)

	scatterState := interactState.NewScatterState()

	for i := 0; i < k; i++ {
		fmt.Println("prepare ", blockNum+uint64(i), "th block")
		blocks[i], headers[i] = utils.GetBlockAndHeader(blockReader, ctx, dbTx, blockNum+uint64(i))
		ibss[i] = utils.GetState(params.MainnetChainConfig, dbTx, blockNum+uint64(i))

		txs, predictRwSets, _ := utils.GetTxsAndPredicts(blockReader, ctx, dbTx, blockNum+uint64(i))
		txss = append(txss, txs...)
		predictRwSetss = append(predictRwSetss, predictRwSets...)

		trueRwSets, err := utils.TrueRWSets(blockReader, ctx, dbTx, blockNum+uint64(i))
		if err != nil {
			// return 0, 0, 0, 0, 0, 0, err
			return
		}

		// 用预测的和真实的rwsets来预取数据构建并发statedb
		scatterState.Prefetch(ibss[i], predictRwSets)
		scatterState.Prefetch(ibss[i], trueRwSets)
	}
	fmt.Println("the batch contains txs len", len(txss))
	blkCtx := utils.GetBlockContext(blockReader, blocks[0], dbTx, headers[0])

	// 初始化全局版本链
	gvc := interactState.NewGlobalVersionChain()

	st := time.Now()
	// 为每个Processor制作状态代理
	states := make([]*interactState.StateForGria, workerNum)
	for i := 0; i < workerNum; i++ {
		states[i] = interactState.NewStateForGria(scatterState, gvc)
	}

	// 贪心分组
	txGroups := gria.GreedyGrouping(txss, workerNum)

	// 制作Processor
	GriaProcessor := make([]*utils.GriaGroupWrapper, workerNum)
	for i := 0; i < workerNum; i++ {
		GriaProcessor[i] = utils.NewGriaGroupWrapper(states[i], txGroups[i], headers[0], blkCtx)
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

	fmt.Println("start to execute the rest of the transactions----------------------------------")
	// 构造新的执行后续交易用的statedb
	os := interactState.NewOuterState(gvc, scatterState)
	// 提取abort的交易
	abortTids := make([]int, 0)
	for i := 0; i < workerNum; i++ {
		abortTids = append(abortTids, GriaProcessor[i].GetAbortTids()...)
	}

	// 获取abort的交易和predictRwset
	abortTxs := make([]types.Transaction, 0)
	abortPredictRwSets := make([]*accesslist.RWSet, 0)

	for _, tid := range abortTids {
		abortTxs = append(abortTxs, txss[tid])
		abortPredictRwSets = append(abortPredictRwSets, predictRwSetss[tid])
	}

	// 生成新的rwAccessedBy
	rwAccessedBy := accesslist.NewRwAccessedBy()
	for i, _ := range abortTxs {
		rwAccessedBy.Add(abortPredictRwSets[i], uint(i))
	}

	// 使用CC并行执行剩余交易
	// 准备线程池
	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(64, ants.WithPreAlloc(true))
	defer antsPool.Release()
	// 建图分组
	graphStart := time.Now()
	// 使用预取RWset构建图
	vIdsGroups := utils.GenerateVertexIdGroups(abortTxs, rwAccessedBy)
	graphTime := time.Since(graphStart)

	groupstart := time.Now()
	// 似乎已经不需要Rwsetgroup了，因为不需要再通过分组进行预取了
	groups, _ := utils.GenerateCCGroups(vIdsGroups, abortTxs, abortPredictRwSets)
	// txGroup, RwSetGroup := utils.GenerateTxAndRWSetGroups(txs, predictRWSet)
	groupTime := time.Since(groupstart)
	createGraphTime := time.Since(graphStart)

	// headers[0].BaseFee = big.NewInt(0)
	// var bf uint256.Int
	// bf.SetFromBig(headers[0].BaseFee)
	// blkCtx.BaseFee = &bf
	// 并发执行
	execStart := time.Now()
	tracer.ExecConflictedTxs(antsPool, groups, os, headers[0], blkCtx, &antsWG)
	execTime := time.Since(execStart)

	// 总时间
	timeSum := time.Since(graphStart)
	maxCost := uint64(0)
	for i := 0; i < len(groups); i++ {
		var temp uint64
		for j := 0; j < len(groups[i]); j++ {
			temp = temp + groups[i][j].GetGas()
		}
		if temp > maxCost {
			maxCost = temp
		}
	}
	fmt.Println("end to execute the rest of the transactions----------------------------------")
	// 输出信息
	fmt.Println("Graph Time:", graphTime)
	fmt.Println("Group Time:", groupTime)
	fmt.Println("Create Graph Time:", createGraphTime)
	fmt.Println("Execution Time:", execTime)
	fmt.Println("Total Time:", timeSum)
	fmt.Println("Max Cost:", maxCost)

}

func main() {

	ctx, dbTx, blockReader, db := utils.PrepareEnv()

	utils.SerialTest(blockReader, ctx, dbTx, 18999999-499)

	utils.CCTest(blockReader, ctx, dbTx, 18999999-499)

	utils.MISTest(blockReader, ctx, dbTx, 18999999-499)
	utils.DAGTest(blockReader, ctx, dbTx, 18999999-499)

	pe := schedule.NewPipeLineExecutor()
	pe.PipeLineExec(blockReader, ctx, db, 18999999-499)

	ApesPtest(blockReader, ctx, dbTx, 18999999-499, 64)
}
