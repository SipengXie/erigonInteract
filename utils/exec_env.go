package utils

import (
	"context"
	interactState "erigonInteract/state"

	"erigonInteract/accesslist"
	"erigonInteract/tracer"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

// PredictRWSets predict a tx rwsets in a block with accesslist
func PredictRWSets(blkCtx evmtypes.BlockContext, header *types.Header, dbTx kv.Tx, tx types.Transaction, blockNum uint64) *accesslist.RWSet {
	ibs := GetState(params.MainnetChainConfig, dbTx, blockNum)
	fulldb := interactState.NewStateWithRwSets(ibs)

	list, err := tracer.ExecToGenerateRWSet(fulldb, tx, header, blkCtx)
	if err != nil {
		// fmt.Println(err)
		fmt.Println("NIL tx hash:", tx.Hash())
	}
	return list
}

// 从前一个区块预测RW sets
func GetTxsAndPredicts(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) (types.Transactions, accesslist.RWSetList) {
	blk, header := GetBlockAndHeader(blockReader, ctx, dbTx, blockNum)

	txs := blk.Transactions()
	predictRwSets := make([]*accesslist.RWSet, txs.Len())

	blkCtx := GetBlockContext(blockReader, blk, dbTx, header)

	for i, tx := range txs {
		predictRwSets[i] = PredictRWSets(blkCtx, header, dbTx, tx, blockNum)
	}
	return txs, predictRwSets
}
