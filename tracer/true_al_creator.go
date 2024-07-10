package tracer

import (
	"erigonInteract/accesslist"
	"erigonInteract/state"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
)

func ExecToGenerateRWSet(fulldb *state.StateWithRwSets, tx types.Transaction, header *types.Header, blkCtx evmtypes.BlockContext) (*accesslist.RWSet, *core.ExecutionResult, error) {
	rwSet := accesslist.NewRWSet()
	fulldb.SetRWSet(rwSet)

	// evm := vm.NewEVM(core.NewEVMBlockContext(header, chainCtx, &header.Coinbase), vm.TxContext{}, fulldb, params.MainnetChainConfig, vm.Config{})
	evm := vm.NewEVM(blkCtx, evmtypes.TxContext{}, fulldb, params.MainnetChainConfig, vm.Config{})
	res, err := ExecuteTx(fulldb, tx, header, evm)
	if err != nil {
		return nil, nil, err
	}
	return rwSet, res, nil
}

// 只服务于True AceessList 不需要返回accessedBy
func CreateRWSetsWithTransactions(fulldb *state.StateWithRwSets, blkCtx evmtypes.BlockContext, txs types.Transactions, header *types.Header) ([]*accesslist.RWSet, []error) {
	ret := make([]*accesslist.RWSet, len(txs))
	err := make([]error, len(txs))

	for i, tx := range txs {
		rws, _, e := ExecToGenerateRWSet(fulldb, tx, header, blkCtx)
		ret[i] = rws
		err[i] = e
		// if res.Err != nil {
		// 	fmt.Println("Error executing transaction in VM layer:", res.Err, "tid:", i)
		// }
	}
	return ret, err
}
