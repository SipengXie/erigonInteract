package schedule

import (
	"context"
	"erigonInteract/accesslist"
	interactState "erigonInteract/state"
	"erigonInteract/tracer"
	"erigonInteract/utils"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
	"github.com/panjf2000/ants/v2"
)

type ScheduleRes struct {
	Flag      uint // 1 for CC , 2 for DAG, 3 for MIS
	cost      uint64
	txsGroups []types.Transactions
	rwsets    []accesslist.RWSetList
	groups    [][]uint

	txs types.Transactions

	// exec env
	blockNum     uint64
	header       *types.Header
	scatterState evmtypes.IntraBlockState
	blkCtx       evmtypes.BlockContext

	// time
	scheduleTime int64
}

type PipeLineExecutor struct {
	Sch chan *ScheduleRes
}

func NewPipeLineExecutor() *PipeLineExecutor {
	pe := &PipeLineExecutor{
		Sch: make(chan *ScheduleRes, 1000),
	}

	return pe
}

func (pe *PipeLineExecutor) PipeLineExecLoop(wg *sync.WaitGroup) error {
	consoleHandler := log.LvlFilterHandler(log.LvlInfo, log.StdoutHandler)
	log.Root().SetHandler(consoleHandler)
	totalTime := int64(0)
	for {
		select {
		case sr := <-pe.Sch:
			switch sr.Flag {
			case 1:
				executeTime, _ := SCC(sr)
				totalTime = totalTime + executeTime + sr.scheduleTime
				log.Info("SCC done", "blockNum", sr.blockNum, "executeTime", executeTime)
			case 2:
				executeTime, _ := SDAG(sr)
				totalTime = totalTime + executeTime + sr.scheduleTime
				log.Info("SDAG done", "blockNum", sr.blockNum, "executeTime", executeTime)

			case 3:
				executeTime, _ := SMIS(sr)
				totalTime = totalTime + executeTime + sr.scheduleTime
				log.Info("SMIS done", "blockNum", sr.blockNum, "executeTime", executeTime)

			case 4:
				wg.Done()
				fmt.Println("Apex exec 10 blocks total time:", totalTime)
				return nil

			default:
				panic("flag error")
			}

		}
	}

}

func (pe *PipeLineExecutor) PipeLineExec(blockReader *freezeblocks.BlockReader, ctx context.Context, db kv.RoDB, blockNum uint64) error {

	consoleHandler := log.LvlFilterHandler(log.LvlInfo, log.StdoutHandler)
	log.Root().SetHandler(consoleHandler)

	var wg sync.WaitGroup
	wg.Add(1)
	go pe.PipeLineExecLoop(&wg)

	for i := blockNum; i < blockNum+500; i++ {
		// 1. getTransacion (seq )
		dbTx1, err := db.BeginRo(ctx)
		if err != nil {
			panic(err)
		}
		block, header := utils.GetBlockAndHeader(blockReader, ctx, dbTx1, i)

		dbTx2, err := db.BeginRo(ctx)
		if err != nil {
			panic(err)
		}
		blkCtx := utils.GetBlockContext(blockReader, block, dbTx2, header)

		dbTx3, err := db.BeginRo(ctx)
		if err != nil {
			panic(err)
		}
		ibs := utils.GetState(params.MainnetChainConfig, dbTx3, i)

		dbTx4, err := db.BeginRo(ctx)
		if err != nil {
			panic(err)
		}
		txs, predictRwSets, rwAccessedBy := utils.GetTxsAndPredicts(blockReader, ctx, dbTx4, i)

		dbTx5, err := db.BeginRo(ctx)
		if err != nil {
			panic(err)
		}
		trueRwSets, err := utils.TrueRWSets(blockReader, ctx, dbTx5, i)

		if err != nil {
			log.Error(err.Error())
			return err
		}

		// 用预测的和真实的rwsets来预取数据构建并发statedb
		scatterState := interactState.NewScatterState()
		scatterState.Prefetch(ibs, predictRwSets)
		scatterState.Prefetch(ibs, trueRwSets)

		// 2. schedule
		st := time.Now()

		sr := Schedule(txs, predictRwSets, rwAccessedBy)
		sr.blkCtx = blkCtx
		sr.header = header
		sr.scatterState = scatterState
		sr.blockNum = i

		scheduleTime := time.Since(st)
		sr.scheduleTime = int64(scheduleTime.Microseconds())

		log.Info("schedule done", "blockNum", i, "scheduleTime", sr.scheduleTime)

		pe.Sch <- sr
	}

	endSr := &ScheduleRes{
		Flag: 4,
	}
	pe.Sch <- endSr
	log.Info("end")

	wg.Wait()
	return nil
}

func Schedule(txs types.Transactions, predictRwSets []*accesslist.RWSet, rwAccessedBy *accesslist.RwAccessedBy) *ScheduleRes {

	var wg sync.WaitGroup
	resultCh := make(chan ScheduleRes, 3)
	wg.Add(2)

	// fmt.Println("CC")
	go CC(txs, predictRwSets, rwAccessedBy, &wg, resultCh)
	// fmt.Println("DAG")
	go DAG(txs, rwAccessedBy, &wg, resultCh)
	// fmt.Println("MIS")
	// go MIS(txs, rwAccessedBy, &wg, resultCh)
	// go MIS(txs, predictRwSets, &wg, resultCh)

	wg.Wait()
	close(resultCh)

	// 获取调度方案
	// fmt.Println("get result")
	var minCost uint64 = math.MaxUint64
	finalRes := new(ScheduleRes)
	for res := range resultCh {
		if res.cost < minCost {
			minCost = res.cost
			temp := res
			finalRes = &temp
		}
	}

	return finalRes
}

// CC 连通分量调度估算
func CC(txs types.Transactions, predictRwSets []*accesslist.RWSet, rwAccessedBy *accesslist.RwAccessedBy, wg *sync.WaitGroup, resultCh chan<- ScheduleRes) {
	defer wg.Done()
	// 无向图
	vertexGroup := utils.GenerateVertexIdGroups(txs, rwAccessedBy)
	// 分组
	txsGroup, RWSetsGroup := utils.GenerateCCGroups(vertexGroup, txs, predictRwSets)
	// 获取最大cost
	var maxCost uint64
	for i := 0; i < len(txsGroup); i++ {
		var temp uint64
		for j := 0; j < len(txsGroup[i]); j++ {
			temp = temp + txsGroup[i][j].GetGas()
		}
		if temp > maxCost {
			maxCost = temp
		}
	}
	// fmt.Println("cc maxCost:", maxCost)
	// 构造返回结构体
	Res := ScheduleRes{
		Flag:         1,
		cost:         maxCost,
		txsGroups:    txsGroup,
		txs:          txs,
		rwsets:       RWSetsGroup,
		groups:       nil,
		header:       nil,
		scatterState: nil,
		blkCtx:       evmtypes.BlockContext{},
	}
	// fmt.Println("Res:", Res)
	resultCh <- Res
}

// DAG 有向无环图调度估算
func DAG(txs types.Transactions, rwAccessedBy *accesslist.RwAccessedBy, wg *sync.WaitGroup, resultCh chan<- ScheduleRes) {
	defer wg.Done()
	groups := utils.GenerateTopoGroups(txs, rwAccessedBy)
	// 获取最大cost
	var maxCost uint64
	maxCost = 0
	for i := 0; i < len(groups); i++ {
		temp := txs[groups[i][0]].GetGas()
		for j := 1; j < len(groups[i]); j++ {
			if temp < txs[groups[i][j]].GetGas() {
				temp = txs[groups[i][j]].GetGas()
			}
		}
		maxCost += temp
	}
	// fmt.Println("dag maxCost:", maxCost)
	// 构造返回结构体
	Res := ScheduleRes{
		Flag:         2,
		cost:         maxCost,
		txsGroups:    nil,
		txs:          txs,
		rwsets:       nil,
		groups:       groups,
		header:       nil,
		scatterState: nil,
		blkCtx:       evmtypes.BlockContext{},
	}
	resultCh <- Res
}

// MIS 最大独立集调度估算
func MIS(txs types.Transactions, predictRwSets []*accesslist.RWSet, wg *sync.WaitGroup, resultCh chan<- ScheduleRes) {
	defer wg.Done()
	// 分组
	// groups := utils.GenerateMISGroups(txs, rwAccessedBy)
	groups := utils.GenerateOldMISGroups(txs, predictRwSets)
	// 获取最大cost
	var maxCost uint64
	maxCost = 0
	for i := 0; i < len(groups); i++ {
		temp := txs[groups[i][0]].GetGas()
		for j := 1; j < len(groups[i]); j++ {
			if temp < txs[groups[i][j]].GetGas() {
				temp = txs[groups[i][j]].GetGas()
			}
		}
		maxCost += temp
	}
	// fmt.Println("mis maxCost:", maxCost)
	// 构造返回结构体
	Res := ScheduleRes{
		Flag:         3,
		cost:         maxCost,
		txsGroups:    nil,
		txs:          txs,
		rwsets:       nil,
		groups:       groups,
		header:       nil,
		scatterState: nil,
		blkCtx:       evmtypes.BlockContext{},
	}
	resultCh <- Res
}

func SCC(sr *ScheduleRes) (int64, error) {
	// 准备线程池
	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(64, ants.WithPreAlloc(true))
	defer antsPool.Release()

	// 并发执行
	execStart := time.Now()
	tracer.ExecConflictedTxs(antsPool, sr.txsGroups, sr.scatterState, sr.header, sr.blkCtx, &antsWG)
	execTime := time.Since(execStart)
	// fmt.Println("cc exec time:", execTime)
	// 预取时间，执行时间，合并时间，总时间
	return int64(execTime.Microseconds()), nil
}

func SDAG(sr *ScheduleRes) (int64, error) {
	// 准备线程池
	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(64, ants.WithPreAlloc(true))
	defer antsPool.Release()

	PureExecutionCost := time.Duration(0)

	for round := 0; round < len(sr.groups); round++ {

		txsToExec := utils.GenerateTxToExec(sr.groups[round], sr.txs)
		execst := time.Now()
		tracer.ExecConflictFreeTxs(antsPool, txsToExec, sr.scatterState, sr.header, sr.blkCtx, &antsWG)
		PureExecutionCost += time.Since(execst)

	}
	// fmt.Println("dag exec time:", PureExecutionCost)
	return int64(PureExecutionCost.Microseconds()), nil
}

func SMIS(sr *ScheduleRes) (int64, error) {
	// 准备线程池
	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	PureExecutionCost := time.Duration(0)

	for round := 0; round < len(sr.groups); round++ {

		txsToExec := utils.GenerateTxToExec(sr.groups[round], sr.txs)
		execst := time.Now()
		tracer.ExecConflictFreeTxs(antsPool, txsToExec, sr.scatterState, sr.header, sr.blkCtx, &antsWG)
		PureExecutionCost += time.Since(execst)
		// fmt.Println("exec time:", time.Since(execst))
	}
	// fmt.Println("mis exec time:", PureExecutionCost)
	// 预取时间，执行时间，合并时间，总时间
	return int64(PureExecutionCost.Microseconds()), nil
}
