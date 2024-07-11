package utils

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
)

const PATH = "/chaindata/erigondata/chaindata"
const LABEL = kv.ChainDB
const SNAPSHOT = "/chaindata/erigondata/snapshots"

func dbCfg(label kv.Label, path string) mdbx.MdbxOpts {
	const ThreadsLimit = 9_000
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	opts := mdbx.NewMDBX(log.New()).Path(path).Label(label).RoTxsLimiter(limiterB)
	opts = opts.Accede()
	return opts
}

// func OpenDB(ctx context.Context) kv.RoDB {
// 	db := dbCfg(LABEL, PATH).MustOpen()
// 	return db
// }

func OpenDB() kv.RoDB {
	db := dbCfg(LABEL, PATH).MustOpen()
	return db
}

func NewBlockReader(cfg ethconfig.Config) *freezeblocks.BlockReader {
	var minFrozenBlock uint64

	if frozenLimit := cfg.Sync.FrozenBlockLimit; frozenLimit != 0 {
		if maxSeedable := snapcfg.MaxSeedableSegment(cfg.Genesis.Config.ChainName, SNAPSHOT); maxSeedable > frozenLimit {
			minFrozenBlock = maxSeedable - frozenLimit
		}
	}

	blockSnaps := freezeblocks.NewRoSnapshots(cfg.Snapshot, SNAPSHOT, minFrozenBlock, log.New())
	borSnaps := freezeblocks.NewBorRoSnapshots(cfg.Snapshot, SNAPSHOT, minFrozenBlock, log.New())
	blockSnaps.ReopenFolder()
	borSnaps.ReopenFolder()
	return freezeblocks.NewBlockReader(blockSnaps, borSnaps)
}

func PrepareEnv() (context.Context, kv.Tx, *freezeblocks.BlockReader, kv.RoDB) {
	consoleHandler := log.LvlFilterHandler(log.LvlInfo, log.StdoutHandler)
	log.Root().SetHandler(consoleHandler)
	log.Info("Starting")
	ctx := context.Background()

	// ready stage
	cfg := ethconfig.Defaults
	// chainConfig := params.MainnetChainConfig
	// db := OpenDB(ctx)
	db := OpenDB()
	// defer db.Close()
	log.Info("DB opened")
	dbTx, err := db.BeginRo(ctx)
	if err != nil {
		log.Error("Failed to begin transaction", "err", err)
		return nil, nil, nil, nil
	}
	log.Info("DB Transaction started")

	blockReader := NewBlockReader(cfg)
	log.Info("Block Reader created")

	return ctx, dbTx, blockReader, db
}

func GetBlockAndHeader(blockReader *freezeblocks.BlockReader, ctx context.Context, dbTx kv.Tx, blockNumber uint64) (*types.Block, *types.Header) {
	consoleHandler := log.LvlFilterHandler(log.LvlInfo, log.StdoutHandler)
	log.Root().SetHandler(consoleHandler)

	blk, err := blockReader.BlockByNumber(ctx, dbTx, blockNumber)
	if err != nil {
		log.Error("Failed to get block", "err", err)
		return nil, nil
	}
	if blk == nil {
		log.Error("Block not found")
		return nil, nil
	}
	// else {
	// 	log.Info("Block found", "block", blk.Number())
	// }

	return blk, blk.Header()
}

func GetState(chainConfig *chain.Config, dbTx kv.Tx, blockNumber uint64) *state.IntraBlockState {
	pls := state.NewPlainState(dbTx, blockNumber, systemcontracts.SystemContractCodeLookup[chainConfig.ChainName])
	ibs := state.New(pls)
	return ibs
}

func GetBlockContext(blockReader *freezeblocks.BlockReader, blk *types.Block, dbTx kv.Tx, header *types.Header) evmtypes.BlockContext {
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, _ := blockReader.Header(context.Background(), dbTx, hash, number)
		return h
	}
	hashFn := core.GetHashFn(header, getHeader)
	coinbase := blk.Coinbase()

	blkCtx := core.NewEVMBlockContext(header, hashFn, nil, &coinbase)
	return blkCtx
}
