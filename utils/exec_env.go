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

	list, _, err := tracer.ExecToGenerateRWSet(fulldb, tx, header, blkCtx)
	if err != nil {
		// fmt.Println(err)
		fmt.Println("NIL tx hash:", tx.Hash())
	}
	return list
}

// 从上一个区块的SNAPSHOT状态预测AccessLists，需要返回AccessedBy辅助建图
func GetTxsAndPredicts(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) (types.Transactions, accesslist.RWSetList, *accesslist.RwAccessedBy) {
	blk, header := GetBlockAndHeader(blockReader, ctx, dbTx, blockNum)
	blkCtx := GetBlockContext(blockReader, blk, dbTx, header)
	txs := blk.Transactions()

	predictRwSets := make([]*accesslist.RWSet, txs.Len())
	rwAccessedBy := accesslist.NewRwAccessedBy()

	for i, tx := range txs {
		predictRwSets[i] = PredictRWSets(blkCtx, header, dbTx, tx, blockNum)
		// 为了建图, 生成对应记录的AccessedBy
		rwAccessedBy.Add(predictRwSets[i], uint(i))
	}
	return txs, predictRwSets, rwAccessedBy
}

func GenerateTxToExec(group []uint, txs types.Transactions) types.Transactions {
	txsToExec := make(types.Transactions, len(group))
	for i := 0; i < len(group); i++ {
		txsToExec[i] = txs[group[i]]
	}
	return txsToExec
}
