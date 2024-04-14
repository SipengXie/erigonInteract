package tracer

import (
	"sync"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
	"github.com/panjf2000/ants/v2"
)

// This function execute without generating tracer.list
func ExecuteTx(ibs evmtypes.IntraBlockState, tx types.Transaction, header *types.Header, evm *vm.EVM) error {

	msg, err := tx.AsMessage(*types.LatestSigner(params.MainnetChainConfig), header.BaseFee, evm.ChainRules())
	if err != nil {
		// This error means the transaction is invalid and should be discarded
		return err
	}

	txContext := core.NewEVMTxContext(msg)
	evm.TxContext = txContext

	// Skip the nonce check!
	msg.SetCheckNonce(false)
	txCtx := core.NewEVMTxContext(msg)
	evm.TxContext = txCtx

	// snapshot := ibs.Snapshot()
	_, err = core.ApplyMessage(evm, msg, new(core.GasPool).AddGas(header.GasLimit), true /* refunds */, false /* gasBailout */)
	// _, err = core.ApplyMessage(evm, msg, new(core.GasPool).AddGas(msg.GasLimit))
	if err != nil {
		// This error means the Execution phase failed and the transaction has been reverted
		return err
	}

	return nil
}

// ExecuteTxs a batch of transactions in a single atomic state transition.
func ExecuteTxs(blkCtx evmtypes.BlockContext, txs types.Transactions, header *types.Header, ibs evmtypes.IntraBlockState) []error {
	evm := vm.NewEVM(blkCtx, evmtypes.TxContext{}, ibs, params.MainnetChainConfig, vm.Config{})
	errs := make([]error, len(txs))
	for i, tx := range txs {
		// ExecBasedOnRWSets includes the snapshot logic
		errs[i] = ExecuteTx(ibs, tx, header, evm)
	}
	return errs
}

// Execute with ants Pool
func ExecConflictedTxs(pool *ants.Pool, txsGroups []types.Transactions, ibs evmtypes.IntraBlockState, header *types.Header, blkCtx evmtypes.BlockContext, wg *sync.WaitGroup) [][]error {
	wg.Add(len(txsGroups))
	errss := make([][]error, len(txsGroups))
	for j := 0; j < len(txsGroups); j++ {
		taskNum := j
		err := pool.Submit(func() {
			errss[taskNum] = ExecuteTxs(blkCtx, txsGroups[taskNum], header, ibs)
			// fmt.Println(errss[taskNum])
			wg.Done() // Mark the task as completed
		})
		if err != nil {
			wg.Done() // Mark the task as completed
		}
	}
	// Wait for all tasks to complete
	wg.Wait()
	return errss
}
