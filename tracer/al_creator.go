package tracer

import (
	"erigonInteract/accesslist"
	"erigonInteract/state"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
)

func ExecToGenerateRWSet(fulldb *state.StateWithRwSets, tx types.Transaction, header *types.Header, blkCtx evmtypes.BlockContext) (*accesslist.RWSet, error) {
	rwSet := accesslist.NewRWSet()
	fulldb.SetRWSet(rwSet)

	// evm := vm.NewEVM(core.NewEVMBlockContext(header, chainCtx, &header.Coinbase), vm.TxContext{}, fulldb, params.MainnetChainConfig, vm.Config{})
	evm := vm.NewEVM(blkCtx, evmtypes.TxContext{}, fulldb, params.MainnetChainConfig, vm.Config{})
	err := ExecuteTx(fulldb, tx, header, evm)
	if err != nil {
		return nil, err
	}
	return rwSet, nil
}

func CreateRWSetsWithTransactions(fulldb *state.StateWithRwSets, blkCtx evmtypes.BlockContext, txs types.Transactions, header *types.Header) ([]*accesslist.RWSet, []error) {
	ret := make([]*accesslist.RWSet, len(txs))
	err := make([]error, len(txs))
	for i, tx := range txs {
		ret[i], err[i] = ExecToGenerateRWSet(fulldb, tx, header, blkCtx)
	}
	return ret, err
}
