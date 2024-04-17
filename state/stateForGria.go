package state

import (
	"erigonInteract/gria"
	"sync"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types"
	coreTypes "github.com/ledgerwatch/erigon/core/types"
)

// State Per Group
// ignore the storageRoot, it can be generated after the all execution of the transactions
// read lw -> cv -> stateSnapshot
// write lw -> gvc/cv
// ignore refund, accesslist, logs, preimages
// simplify selfdestruct, revert, snapshot
// the codeHash could be optimized as we do not store the code at other place, no need to have a secondary index
// but now, ignore it.
type StateForGria struct {
	stateSnapshot *ScatterState       // global view: statedb snapshot, different gourps may access it concurrently, so it has to be concurrency-friendly
	gvc           *globalVersionChain // global view: version chain per record

	cv *MapVersion // group view: current version per record, either the -1 version or the latest version in this group

	rv *MapVersion // Tx view: readset per record

	lw  *LocalWrite // Tx view: localWrite per record
	tid int

	wv *MapVersion // Tx view: writeset per record, generated after commit
}

// called per group
func NewStateForGria(statedb *ScatterState, gvc *globalVersionChain) *StateForGria {
	return &StateForGria{
		stateSnapshot: statedb,
		gvc:           gvc,
		cv:            newMapVersion(gvc),
	}
}

// called before execution to generate multi-version records
func (sfg *StateForGria) SetTxContext(_ common.Hash, ti int) {
	sfg.rv = newMapVersion(sfg.gvc)
	sfg.wv = newMapVersion(sfg.gvc)
	sfg.lw = newLocalWrite()
	sfg.tid = ti
}

// ----------------- Getters for StateForGria -----------------------

// called inside a transaction, read workflow
// lw - curVersion - stateSnapshot
func (sfg *StateForGria) GetBalance(addr common.Address) *uint256.Int {
	balance, ok := sfg.lw.getBalance(addr)
	if ok {
		return balance
	}
	// cannot read from localWrite, read from curVersion
	cur_v := sfg.cv.getBalance(addr)

	// update the readVersion
	sfg.rv.setBalance(addr, cur_v)
	// update the readby and maxReadBy if cur_v.tid >= 0 (cur_v is generated by transactions)
	if cur_v.Tid >= 0 {
		cur_v.Readby[sfg.tid] = struct{}{}
		if sfg.tid > cur_v.MaxReadby {
			cur_v.MaxReadby = sfg.tid
		}
	}
	if cur_v.Data != nil {
		data := cur_v.Data.(uint256.Int)
		return &data
	}
	// cannot read from curVersion or does not been read before, read from stateSnapshot
	balance = sfg.stateSnapshot.GetBalance(addr)
	cur_v.Data = *balance
	return balance
}

// called inside a transaction, read workflow
func (sfg *StateForGria) GetNonce(addr common.Address) uint64 {
	nonce, ok := sfg.lw.getNonce(addr)
	if ok {
		return nonce
	}
	cur_v := sfg.cv.getNonce(addr)
	sfg.rv.setNonce(addr, cur_v)
	if cur_v.Tid >= 0 {
		cur_v.Readby[sfg.tid] = struct{}{}
		if sfg.tid > cur_v.MaxReadby {
			cur_v.MaxReadby = sfg.tid
		}
	}
	if cur_v.Data != nil {
		return cur_v.Data.(uint64)
	}
	nonce = sfg.stateSnapshot.GetNonce(addr)
	cur_v.Data = nonce
	return nonce
}

func (sfg *StateForGria) GetCodeHash(addr common.Address) common.Hash {
	codeHash, ok := sfg.lw.getCodeHash(addr)
	if ok {
		return codeHash
	}
	cur_v := sfg.cv.getCodeHash(addr)
	if cur_v.Tid >= 0 {
		cur_v.Readby[sfg.tid] = struct{}{}
		if sfg.tid > cur_v.MaxReadby {
			cur_v.MaxReadby = sfg.tid
		}
	}
	sfg.rv.setCodeHash(addr, cur_v)
	if cur_v.Data != nil {
		return cur_v.Data.(common.Hash)
	}
	codeHash = sfg.stateSnapshot.GetCodeHash(addr)
	cur_v.Data = codeHash
	return codeHash
}

func (sfg *StateForGria) GetCode(addr common.Address) []byte {
	code, ok := sfg.lw.getCode(addr)
	if ok {
		return code
	}
	cur_v := sfg.cv.getCode(addr)
	if cur_v.Tid >= 0 {
		cur_v.Readby[sfg.tid] = struct{}{}
		if sfg.tid > cur_v.MaxReadby {
			cur_v.MaxReadby = sfg.tid
		}
	}
	sfg.rv.setCode(addr, cur_v)
	if cur_v.Data != nil {
		return cur_v.Data.([]byte)
	}
	code = sfg.stateSnapshot.GetCode(addr)
	cur_v.Data = code
	return code
}

func (sfg *StateForGria) GetCodeSize(addr common.Address) int {
	return len(sfg.GetCode(addr))
}

func (sfg *StateForGria) GetState(addr common.Address, hash *common.Hash, ret *uint256.Int) {
	value, ok := sfg.lw.getStorage(addr, *hash)
	if ok {
		ret.Set(value)
		return
	}
	cur_v := sfg.cv.getStorage(addr, *hash)
	if cur_v.Tid >= 0 {
		cur_v.Readby[sfg.tid] = struct{}{}
		if sfg.tid > cur_v.MaxReadby {
			cur_v.MaxReadby = sfg.tid
		}
	}
	sfg.rv.setStorage(addr, *hash, cur_v)
	if cur_v.Data != nil {
		data := cur_v.Data.(uint256.Int)
		*ret = data
		return
	}
	sfg.stateSnapshot.GetState(addr, hash, ret)
	cur_v.Data = *ret
}

func (sfg *StateForGria) HasSelfdestructed(addr common.Address) bool {
	if !sfg.Exist(addr) {
		return false
	}
	alive, ok := sfg.lw.getAlive(addr)
	if ok {
		return !alive
	}
	cur_v := sfg.cv.getAlive(addr)
	if cur_v.Tid >= 0 {
		cur_v.Readby[sfg.tid] = struct{}{}
		if sfg.tid > cur_v.MaxReadby {
			cur_v.MaxReadby = sfg.tid
		}
	}
	sfg.rv.setAlive(addr, cur_v)
	if cur_v.Data != nil {
		return !cur_v.Data.(bool)
	}

	dead := sfg.stateSnapshot.HasSelfdestructed(addr)
	cur_v.Data = !dead
	return dead
}

func (sfg *StateForGria) GetCommittedState(addr common.Address, hash *common.Hash, value *uint256.Int) {
	sfg.GetState(addr, hash, value)
}

// ignore the transient state
func (sfg *StateForGria) GetTransientState(addr common.Address, key common.Hash) uint256.Int {
	// TODO: Implement
	return *uint256.NewInt(0)
}

func (sfg *StateForGria) GetRefund() uint64 {
	// TODO: Implement
	return 0
}

// Exist reports whether the given account exists in state.
// Notably this should also return true for self-destructed accounts.
// this function we do not count it into the readset as its a basic check, and most of time it will return false
func (sfg *StateForGria) Exist(addr common.Address) bool {
	_, ok := sfg.lw.getBalance(addr)
	if ok {
		return true
	}
	if sfg.cv.getBalance(addr) != nil {
		return true
	}
	return sfg.stateSnapshot.Exist(addr)
}

// Empty returns whether the given account is empty. Empty
// is defined according to EIP161 (balance = nonce = code = 0).
// this function we do not count it into the readset as its a basic check, and most of time it will return false
func (sfg *StateForGria) Empty(addr common.Address) bool {
	if !sfg.Exist(addr) {
		return true
	}
	balance := sfg.GetBalance(addr)
	if balance.Sign() != 0 {
		return false
	}
	nonce := sfg.GetNonce(addr)
	if nonce != 0 {
		return false
	}
	codesize := sfg.GetCodeSize(addr)
	return codesize == 0
}

// ignore
func (sfg *StateForGria) AddressInAccessList(addr common.Address) bool {
	// TODO: Implement
	return true
}

// ignore
func (sfg *StateForGria) SlotInAccessList(addr common.Address, slot common.Hash) (addressOk bool, slotOk bool) {
	// TODO: Implement
	return true, true
}

// ----------------- Setters for StateForGria -----------------------

// called inside a transaction
func (sfg *StateForGria) CreateAccount(addr common.Address, _ bool) {
	sfg.lw.createAccount(addr)
}

func (sfg *StateForGria) SetBalance(addr common.Address, value *uint256.Int) {
	sfg.lw.setBalance(addr, value)
}

func (sfg *StateForGria) SetNonce(addr common.Address, nonce uint64) {
	sfg.lw.setNonce(addr, nonce)
}

func (sfg *StateForGria) SetCode(addr common.Address, code []byte) {
	sfg.lw.setCode(addr, code)
}

func (sfg *StateForGria) SetState(addr common.Address, hash *common.Hash, value uint256.Int) {
	sfg.lw.setStorage(addr, *hash, value)
}

// called inside a transaction, R-M-W workflow
// GetBalance - sub - SetBalance
func (sfg *StateForGria) SubBalance(addr common.Address, value *uint256.Int) {
	balance := sfg.GetBalance(addr)
	balance.Sub(balance, value)
	sfg.SetBalance(addr, balance)
}

func (sfg *StateForGria) AddBalance(addr common.Address, value *uint256.Int) {
	balance := sfg.GetBalance(addr)
	balance.Add(balance, value)
	sfg.SetBalance(addr, balance)
}

// ignore
func (sfg *StateForGria) AddRefund(_ uint64) {
	// TODO: Implement
}

// ignore
func (sfg *StateForGria) SubRefund(_ uint64) {
	// TODO: Implement
}

// ignore
func (sfg *StateForGria) SetTransientState(addr common.Address, key common.Hash, value uint256.Int) {
	// TODO: Implement
}

func (sfg *StateForGria) Selfdestruct(addr common.Address) bool {
	sfg.SetBalance(addr, uint256.NewInt(0))
	sfg.lw.setAlive(addr, false)
	return true
}

func (sfg *StateForGria) Selfdestruct6780(addr common.Address) {
	sfg.Selfdestruct(addr)
}

// AddAddressToAccessList adds the given address to the access list. This operation is safe to perform
// even if the feature/fork is not active yet
// ignore
func (sfg *StateForGria) AddAddressToAccessList(addr common.Address) bool {
	return false
	// TODO: Implement
}

// AddSlotToAccessList adds the given (address,slot) to the access list. This operation is safe to perform
// even if the feature/fork is not active yet
// ignore
func (sfg *StateForGria) AddSlotToAccessList(addr common.Address, slot common.Hash) (bool, bool) {
	return false, false
	// TODO: Implement
}

// ignore
func (sfg *StateForGria) Prepare(rules *chain.Rules, sender common.Address, coinbase common.Address, dest *common.Address, precompiles []common.Address, txAccesses types.AccessList) {
	// TODO: Implement
}

// simplify
func (sfg *StateForGria) RevertToSnapshot(_ int) {
	sfg.lw = newLocalWrite() // TODO: Implement
}

// ignore
func (sfg *StateForGria) Snapshot() int {
	return 0
	// TODO: Implement
}

// ignore
func (sfg *StateForGria) AddLog(*coreTypes.Log) {
	// TODO: Implement
}

// ignore
func (sfg *StateForGria) AddPreimage(_ common.Hash, _ []byte) {
	// TODO: Implement
}

func (sfg *StateForGria) mergeBalance(wait *sync.WaitGroup) {
	defer wait.Done()
	for addr, balance := range sfg.lw.localBalance {
		iv := gria.NewVersion(balance, sfg.tid, gria.Pending)
		sfg.wv.setBalance(addr, iv)
		sfg.cv.setBalance(addr, iv)
		sfg.gvc.insertBalanceVersion(addr, iv)
	}
}

func (sfg *StateForGria) mergeNonce(wait *sync.WaitGroup) {
	defer wait.Done()
	for addr, nonce := range sfg.lw.localNonce {
		iv := gria.NewVersion(nonce, sfg.tid, gria.Pending)
		sfg.wv.setNonce(addr, iv)
		sfg.cv.setNonce(addr, iv)
		sfg.gvc.insertNonceVersion(addr, iv)
	}

}

func (sfg *StateForGria) mergeCode(wait *sync.WaitGroup) {
	defer wait.Done()
	for addr, code := range sfg.lw.localCode {
		iv := gria.NewVersion(code, sfg.tid, gria.Pending)
		sfg.wv.setCode(addr, iv)
		sfg.cv.setCode(addr, iv)
		sfg.gvc.insertCodeVersion(addr, iv)
	}
}

func (sfg *StateForGria) mergeAlive(wait *sync.WaitGroup) {
	defer wait.Done()
	for addr, alive := range sfg.lw.localAlive {
		iv := gria.NewVersion(alive, sfg.tid, gria.Pending)
		sfg.wv.setAlive(addr, iv)
		sfg.cv.setAlive(addr, iv)
		sfg.gvc.insertAliveVersion(addr, iv)
	}
}

func (sfg *StateForGria) mergeStorage(wait *sync.WaitGroup) {
	defer wait.Done()
	for addr, cache := range sfg.lw.localStorage {
		for hash, value := range cache {
			iv := gria.NewVersion(value, sfg.tid, gria.Pending)
			sfg.wv.setStorage(addr, hash, iv)
			sfg.cv.setStorage(addr, hash, iv)
			sfg.gvc.insertStorageVersion(addr, hash, iv)
		}
	}
}

func (sfg *StateForGria) mergeCodeHash(wait *sync.WaitGroup) {
	defer wait.Done()
	for addr, codeHash := range sfg.lw.localCodeHash {
		iv := gria.NewVersion(codeHash, sfg.tid, gria.Pending)
		sfg.wv.setCodeHash(addr, iv)
		sfg.cv.setCodeHash(addr, iv)
		sfg.gvc.insertCodeHashVersion(addr, iv)
	}
}

// insert local writes to the gvc
// and update the curVersion
// six records could be updated concurrently
func (sfg *StateForGria) Commit() {
	var wg sync.WaitGroup
	wg.Add(6)
	go sfg.mergeBalance(&wg)
	go sfg.mergeNonce(&wg)
	go sfg.mergeCode(&wg)
	go sfg.mergeAlive(&wg)
	go sfg.mergeStorage(&wg)
	go sfg.mergeCodeHash(&wg)
	wg.Wait()
}

func (sfg *StateForGria) GetReadSet() *MapVersion {
	return sfg.rv
}

func (sfg *StateForGria) GetWriteSet() *MapVersion {
	return sfg.wv
}
