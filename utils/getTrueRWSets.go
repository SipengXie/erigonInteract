package utils

import (
	"context"
	"erigonInteract/accesslist"
	interactState "erigonInteract/state"
	"erigonInteract/tracer"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

func TrueRWSets(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNum uint64) (accesslist.RWSetList, error) {
	blk, header := GetBlockAndHeader(blockReader, ctx, dbTx, blockNum)
	ibs := GetState(params.MainnetChainConfig, dbTx, blockNum)
	fulldb := interactState.NewStateWithRwSets(ibs)
	txs := blk.Transactions()
	lists, errs := tracer.CreateRWSetsWithTransactions(fulldb, GetBlockContext(blockReader, blk, dbTx, header), txs, header)
	for i, err := range errs {
		if err != nil {
			fmt.Println("In TRUERWSetsS, tx hash:", txs[i].Hash())
		}
	}

	var rlen, wlen int = 0, 0
	// 统计RWSet
	for _, list := range lists {
		rlen += len(list.ReadSet)
		wlen += len(list.WriteSet)
	}
	// fmt.Println("blockNumber:", blockNum, "ReadSet length:", rlen, ", WriteSet length:", wlen)
	// fmt.Println("blockNumber:", blockNum, "RWSet sum", rlen+wlen)
	return lists, nil
}
