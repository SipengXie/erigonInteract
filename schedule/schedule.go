package schedule

import (
	"erigonInteract/accesslist"
	"erigonInteract/utils"
	"fmt"
	"math"
	"sync"

	"github.com/ledgerwatch/erigon/core/types"
)

type ScheduleRes struct {
	Flag   uint // 1 for CC , 2 for DAG, 3 for MIS
	cost   uint64
	txs    []types.Transactions
	rwsets []accesslist.RWSetList
	groups [][]uint
}

type PipeLineExecutor struct {
	sch chan *ScheduleRes

	wg sync.WaitGroup // for go-routine
}

func NewPipeLineExecutor() *PipeLineExecutor {
	pe := &PipeLineExecutor{
		sch: make(chan *ScheduleRes, 1000),
	}

	// pe.wg.Add(1)
	// go pe.PipeLineExecLoop()
	return pe
}

func (pe *PipeLineExecutor) PipeLineSchedule(txs types.Transactions, predictRwSets []*accesslist.RWSet) error {
	var wg sync.WaitGroup
	resultCh := make(chan ScheduleRes, 3)
	wg.Add(3)

	// fmt.Println("CC")
	go CC(txs, predictRwSets, &wg, resultCh)
	// fmt.Println("DAG")
	go DAG(txs, predictRwSets, &wg, resultCh)
	// fmt.Println("MIS")
	go MIS(txs, predictRwSets, &wg, resultCh)

	wg.Wait()
	close(resultCh)

	// 获取调度方案
	fmt.Println("get result")
	var minCost uint64 = math.MaxUint64
	finalRes := new(ScheduleRes)
	for res := range resultCh {
		if res.cost < minCost {
			minCost = res.cost
			temp := res
			finalRes = &temp
		}
	}

	pe.sch <- finalRes

	return nil
}

// CC 连通分量调度估算
func CC(txs types.Transactions, predictRwSets []*accesslist.RWSet, wg *sync.WaitGroup, resultCh chan<- ScheduleRes) {
	defer wg.Done()
	// 无向图
	vertexGroup := utils.GenerateVertexGroups(txs, predictRwSets)
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
	fmt.Println("cc maxCost:", maxCost)
	// 构造返回结构体
	Res := ScheduleRes{
		Flag:   1,
		cost:   maxCost,
		txs:    txsGroup,
		rwsets: RWSetsGroup,
		groups: nil,
	}
	// fmt.Println("Res:", Res)
	resultCh <- Res
}

// DAG 有向无环图调度估算
func DAG(txs types.Transactions, predictRwSets []*accesslist.RWSet, wg *sync.WaitGroup, resultCh chan<- ScheduleRes) {
	defer wg.Done()
	groups := utils.GenerateDegreeZeroGroups(txs, predictRwSets)
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
	fmt.Println("dag maxCost:", maxCost)
	// 构造返回结构体
	Res := ScheduleRes{
		Flag:   2,
		cost:   maxCost,
		txs:    nil,
		rwsets: nil,
		groups: groups,
	}
	resultCh <- Res
}

// MIS 最大独立集调度估算
func MIS(txs types.Transactions, predictRwSets []*accesslist.RWSet, wg *sync.WaitGroup, resultCh chan<- ScheduleRes) {
	defer wg.Done()
	// 分组
	groups := utils.GenerateMISGroups(txs, predictRwSets)
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
	fmt.Println("mis maxCost:", maxCost)
	// 构造返回结构体
	Res := ScheduleRes{
		Flag:   3,
		cost:   maxCost,
		txs:    nil,
		rwsets: nil,
		groups: groups,
	}
	resultCh <- Res
}
