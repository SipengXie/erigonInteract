package main

import (
	"erigonInteract/utils"
)

func main() {
	// ctx, dbTx, blockReader := utils.PrepareEnv()

	// blk, header := utils.GetBlockAndHeader(blockReader, ctx, dbTx, 18999998)

	// fmt.Println("Block", blk.Hash(), "Header", header.Hash())

	// txs := blk.Transactions()

	// gria.GreedyGrouping(txs, 4)

	// // stateReader
	// // pls := state.NewPlainState(dbTx, 18999999, systemcontracts.SystemContractCodeLookup[chainConfig.ChainName])
	// // ibs := state.New(pls)
	// chainConfig := params.MainnetChainConfig
	// ibs := utils.GetState(chainConfig, dbTx, 18999999)
	// log.Info("Nonce", "nonce", ibs.GetNonce(common.HexToAddress("0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13")))

	// // Serial Execution
	// st := time.Now()
	// // header := blk.Header()
	// getHeader := func(hash common.Hash, number uint64) *types.Header {
	// 	h, _ := blockReader.Header(context.Background(), dbTx, hash, number)
	// 	return h
	// }
	// hashFn := core.GetHashFn(header, getHeader)
	// coinbase := blk.Coinbase()

	// txs := blk.Transactions()
	// log.Info("Transactions", "count", len(txs))

	// blkCtx := core.NewEVMBlockContext(header, hashFn, nil, &coinbase)
	// vmCfg := vm.Config{}
	// gp := new(core.GasPool)
	// gp.AddGas(header.GasLimit)

	// evm := vm.NewEVM(blkCtx, evmtypes.TxContext{}, ibs, chainConfig, vmCfg)
	// rule := evm.ChainRules()
	// signer := types.LatestSigner(chainConfig)
	// for i, tx := range txs {
	// 	log.Info("Executing transaction", "index", i)
	// 	msg, _ := tx.AsMessage(*signer, header.BaseFee, rule)
	// 	txContext := core.NewEVMTxContext(msg)
	// 	evm.TxContext = txContext
	// 	_, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
	// 	if err != nil {
	// 		log.Error("Failed to apply message", "err", err)
	// 	}
	// }
	// log.Info("Execution time", "time", time.Since(st))

	ctx, dbTx, blockReader := utils.PrepareEnv()
	// utils.SerialTest(blockReader, ctx, dbTx, 18999999-10000)
	// utils.SerialExec(blockReader, ctx, dbTx, 18999999-10000)
	// utils.CCTest(blockReader, ctx, dbTx, 18999999-10000)
	utils.CCExec(blockReader, ctx, dbTx, 18999999-10000)
	// utils.MISTest(blockReader, ctx, dbTx, 18999999-10000)
	// utils.DAGTest(blockReader, ctx, dbTx, 18999999-10000)
	// utils.DAGExec(blockReader, ctx, dbTx, 18999999-10000)
	// utils.GriaExec(blockReader, ctx, dbTx, 18999999-10000, 4)
}
