package utils

import (

	// "interact/core"
	"erigonInteract/gria"
	"erigonInteract/state"
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
)

func (w *GriaGroupWrapper) processTx(tx types.Transaction, tid int, header *types.Header, evm *vm.EVM) (*core.ExecutionResult, error) {
	msg, err := tx.AsMessage(*types.LatestSigner(params.MainnetChainConfig), header.BaseFee, evm.ChainRules())
	if err != nil {
		// This error means the transaction is invalid and should be discarded
		return nil, err
	}

	// txContext := core.NewEVMTxContext(msg)
	// evm.TxContext = txContext

	// Skip the nonce check!
	msg.SetCheckNonce(false)
	txCtx := core.NewEVMTxContext(msg)
	evm.TxContext = txCtx
	w.state.SetTxContext(common.Hash{}, tid)
	msg.SetIsFree(true)
	// snapshot := ibs.Snapshot()
	// !! 实际上这里应该有两个层次的Revert，合约层次与EOA层次，在StateForGria中，revert只是修改LocalWrite，实际上是可以做到正确的Revert逻辑的
	// !! 但我们先忽略这一点
	res, err := core.ApplyMessage(evm, msg, new(core.GasPool).AddGas(header.GasLimit), true /* refunds */, false /* gasBailout */)
	// _, err = core.ApplyMessage(evm, msg, new(core.GasPool).AddGas(msg.GasLimit))
	if err != nil {
		return nil, err
	}

	// insert versions to the global version chain
	w.state.Commit()
	return res, err

}

// combine reordering and cascade abort
func (w *GriaGroupWrapper) canCommit(tid int) bool {
	// cascade abort
	if _, ok := w.abort[tid]; ok {
		w.cascadeAbort(tid)
		return false
	}

	// if w.readVersions[tid] == nil {
	// 	// w.cascadeAbort(tid)
	// 	return false
	// }

	max_r, min_rn := w.readVersions[tid].ScanRead()
	max_wp := w.writeVersions[tid].ScanWrite()

	if tid < min_rn {
		return true
	} else {
		if max_wp < min_rn && max_r < min_rn {
			return true
		} else {
			w.cascadeAbort(tid)
			return false
		}
	}
}

func (w *GriaGroupWrapper) cascadeAbort(tid int) {
	w.abort[tid] = struct{}{}
	writeSet := w.writeVersions[tid]
	readBys := writeSet.GetAllReadbys()
	for _, readby := range readBys {
		w.abort[readby] = struct{}{}
	}
}

// if is executed based on the tid, the deadlock will not happen
func (w *GriaGroupWrapper) recheck(tid int) bool {
	rv := w.readVersions[tid].GetReads()
	for _, v := range rv {
		for nv := v.Next; nv != nil; nv = nv.Next {
			if nv.Tid < tid {
				// wait for next visible version to deicide
				for nv.Status == gria.Pending {
					continue
				}

				if nv.Status == gria.Committed {
					w.writeVersions[tid].SetStatus(gria.Aborted)
					return false
				}
			} else {
				break
			}
		}
		// wait for current version to decide
		for v.Status == gria.Pending {
			continue
		}
		if v.Status == gria.Aborted {
			w.writeVersions[tid].SetStatus(gria.Aborted)
			return false
		}
	}
	w.writeVersions[tid].SetStatus(gria.Committed)
	return true
}

func (w *GriaGroupWrapper) GetAbortNum() int {
	return len(w.abort)
}

// each group the transactions is ordered based on the tid
type GriaGroupWrapper struct {
	state  *state.StateForGria
	txs    gria.SortingTxs
	header *types.Header
	blkCtx evmtypes.BlockContext
	// tid -> read/write set
	readVersions  map[int]*state.MapVersion
	writeVersions map[int]*state.MapVersion
	// tid -> aborted?
	abort map[int]struct{}
}

func NewGriaGroupWrapper(state *state.StateForGria, txs gria.SortingTxs, header *types.Header, blkCtx evmtypes.BlockContext) *GriaGroupWrapper {
	return &GriaGroupWrapper{
		state:  state,
		txs:    txs,
		header: header,
		blkCtx: blkCtx,
	}
}

// after all ProcessTxs finish, then we can do the reordering & committing & rechecking algorithm
func (w *GriaGroupWrapper) ProcessTxs(wait *sync.WaitGroup) {
	defer wait.Done()
	evm := vm.NewEVM(w.blkCtx, evmtypes.TxContext{}, w.state, params.MainnetChainConfig, vm.Config{})
	rvs := make(map[int]*state.MapVersion)
	wvs := make(map[int]*state.MapVersion)
	fmt.Println(len(w.txs))
	for _, txWithIndex := range w.txs {
		// if txWithIndex.Tid == 241 {
		// 	fmt.Println("bingo tid = 241 i'm here")
		// }
		res, err := w.processTx(txWithIndex.Tx, txWithIndex.Tid, w.header, evm)
		if err != nil {
			fmt.Println("Fatal error:", err, "tid:", txWithIndex.Tid)
			continue
		}
		if res.Err != nil {
			fmt.Println("Error executing transaction in VM layer:", res.Err, "tid:", txWithIndex.Tid)
		}

		rvs[txWithIndex.Tid] = w.state.GetReadSet()
		wvs[txWithIndex.Tid] = w.state.GetWriteSet()
		// if txWithIndex.Tid == 5 {
		// 	fmt.Println("bingo tid = 5", w.state.GetReadSet())
		// }
		// if txWithIndex.Tid == 241 {
		// 	fmt.Println("bingo tid = 241", w.state.GetReadSet())
		// }
	}
	w.readVersions = rvs
	w.writeVersions = wvs
	w.abort = make(map[int]struct{})
}

func (w *GriaGroupWrapper) CommitTxs(wait *sync.WaitGroup) {
	defer wait.Done()
	for _, txWithIndex := range w.txs {

		if w.writeVersions[txWithIndex.Tid] == nil && w.readVersions[txWithIndex.Tid] == nil {
			w.abort[txWithIndex.Tid] = struct{}{}
			continue
		}

		if w.canCommit(txWithIndex.Tid) {
			w.writeVersions[txWithIndex.Tid].SetStatus(gria.Committed)
		} else {
			w.writeVersions[txWithIndex.Tid].SetStatus(gria.Aborted)
		}
	}
}

func (w *GriaGroupWrapper) RecheckTxs(wait *sync.WaitGroup) {
	defer wait.Done()
	for _, txWithIndex := range w.txs {

		if w.writeVersions[txWithIndex.Tid] == nil && w.readVersions[txWithIndex.Tid] == nil {
			continue
		}

		if _, ok := w.abort[txWithIndex.Tid]; ok {
			if w.recheck(txWithIndex.Tid) {
				delete(w.abort, txWithIndex.Tid)
			}
		}
	}
}

func (w *GriaGroupWrapper) GetAbortTids() []int {
	abortTids := make([]int, 0)
	for tid, _ := range w.abort {
		abortTids = append(abortTids, tid)
	}
	return abortTids
}

func (w *GriaGroupWrapper) GetAbortTxs(txs types.Transactions) types.Transactions {
	abortTids := w.GetAbortTids()

	abortTxs := make(types.Transactions, len(abortTids))
	for i, tid := range abortTids {
		fmt.Println("Abort tx tid:", tid)
		abortTxs[i] = txs[tid]
	}
	return abortTxs
}
