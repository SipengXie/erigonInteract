package state

import (
	"erigonInteract/accesslist"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
)

type StateWithRwSets struct {
	StateDb *state.IntraBlockState
	RwSets  *accesslist.RWSet
}

func NewStateWithRwSets(ibs *state.IntraBlockState) *StateWithRwSets {
	return &StateWithRwSets{
		StateDb: ibs,
		RwSets:  nil,
	}
}

// ----------------------- Getters ----------------------------
func (fs *StateWithRwSets) GetStateDb() *state.IntraBlockState {
	return fs.StateDb
}

func (fs *StateWithRwSets) GetRWSet() *accesslist.RWSet {
	return fs.RwSets
}

func (fs *StateWithRwSets) GetBalance(addr common.Address) *uint256.Int {
	if fs.RwSets != nil {
		fs.RwSets.AddReadSet(addr, accesslist.BALANCE)
	}
	return fs.StateDb.GetBalance(addr)
}

func (fs *StateWithRwSets) GetNonce(addr common.Address) uint64 {
	if fs.RwSets != nil {
		fs.RwSets.AddReadSet(addr, accesslist.NONCE)
	}
	return fs.StateDb.GetNonce(addr)
}

func (fs *StateWithRwSets) GetCodeHash(addr common.Address) common.Hash {
	if fs.RwSets != nil {
		fs.RwSets.AddReadSet(addr, accesslist.CODEHASH)
	}
	return fs.StateDb.GetCodeHash(addr)
}

func (fs *StateWithRwSets) GetCode(addr common.Address) []byte {
	if fs.RwSets != nil {
		fs.RwSets.AddReadSet(addr, accesslist.CODE)
	}
	return fs.StateDb.GetCode(addr)
}

func (fs *StateWithRwSets) GetCodeSize(addr common.Address) int {
	if fs.RwSets != nil {
		fs.RwSets.AddReadSet(addr, accesslist.CODE)
	}
	return fs.StateDb.GetCodeSize(addr)
}

func (fs *StateWithRwSets) GetRefund() uint64 {
	return fs.StateDb.GetRefund()
}

func (fs *StateWithRwSets) GetCommittedState(addr common.Address, key *common.Hash, value *uint256.Int) {
	if fs.RwSets != nil {
		fs.RwSets.AddReadSet(addr, *key)
	}
	fs.StateDb.GetCommittedState(addr, key, value)
}

func (fs *StateWithRwSets) GetState(addr common.Address, key *common.Hash, value *uint256.Int) {
	if fs.RwSets != nil {
		fs.RwSets.AddReadSet(addr, *key)
	}
	fs.StateDb.GetState(addr, key, value)
}

func (fs *StateWithRwSets) GetTransientState(addr common.Address, key common.Hash) uint256.Int {
	if fs.RwSets != nil {
		fs.RwSets.AddReadSet(addr, key)
	}
	return fs.StateDb.GetTransientState(addr, key)
}

func (fs *StateWithRwSets) HasSelfdestructed(addr common.Address) bool {
	if fs.RwSets != nil {
		fs.RwSets.AddReadSet(addr, accesslist.ALIVE)
	}
	return fs.StateDb.HasSelfdestructed(addr)
}

func (fs *StateWithRwSets) Exist(addr common.Address) bool {
	return fs.StateDb.Exist(addr)
}

func (fs *StateWithRwSets) Empty(addr common.Address) bool {
	return fs.StateDb.Empty(addr)
}

// ----------------------- Setters ----------------------------
func (fs *StateWithRwSets) SetStateDb(StateDb *state.IntraBlockState) {
	fs.StateDb = StateDb
}

func (fs *StateWithRwSets) SetRWSet(RwSets *accesslist.RWSet) {
	fs.RwSets = RwSets
}

func (fs *StateWithRwSets) CreateAccount(addr common.Address, contractCreation bool) {
	fs.StateDb.CreateAccount(addr, contractCreation)
}

func (fs *StateWithRwSets) AddBalance(addr common.Address, amount *uint256.Int) {
	if fs.RwSets != nil {
		fs.RwSets.AddWriteSet(addr, accesslist.BALANCE)
	}
	fs.StateDb.AddBalance(addr, amount)
}

func (fs *StateWithRwSets) SubBalance(addr common.Address, amount *uint256.Int) {
	if fs.RwSets != nil {
		fs.RwSets.AddWriteSet(addr, accesslist.BALANCE)
	}
	fs.StateDb.SubBalance(addr, amount)
}

func (fs *StateWithRwSets) SetBalance(addr common.Address, amount *uint256.Int) {
	if fs.RwSets != nil {
		fs.RwSets.AddWriteSet(addr, accesslist.BALANCE)
	}
	fs.StateDb.SetBalance(addr, amount)
}

func (fs *StateWithRwSets) SetNonce(addr common.Address, nonce uint64) {
	if fs.RwSets != nil {
		fs.RwSets.AddWriteSet(addr, accesslist.NONCE)
	}
	fs.StateDb.SetNonce(addr, nonce)
}

func (fs *StateWithRwSets) SetCode(addr common.Address, code []byte) {
	if fs.RwSets != nil {
		fs.RwSets.AddWriteSet(addr, accesslist.CODE)
		fs.RwSets.AddWriteSet(addr, accesslist.CODEHASH)
	}
	fs.StateDb.SetCode(addr, code)
}

func (fs *StateWithRwSets) SetState(addr common.Address, key *common.Hash, value uint256.Int) {
	if fs.RwSets != nil {
		fs.RwSets.AddWriteSet(addr, *key)
	}
	fs.StateDb.SetState(addr, key, value)
}

func (fs *StateWithRwSets) SetTransientState(addr common.Address, key common.Hash, value uint256.Int) {
	if fs.RwSets != nil {
		fs.RwSets.AddWriteSet(addr, key)
	}
	fs.StateDb.SetTransientState(addr, key, value)
}

func (fs *StateWithRwSets) Selfdestruct(addr common.Address) bool {
	if fs.RwSets != nil {
		fs.RwSets.AddWriteSet(addr, accesslist.ALIVE)
		fs.RwSets.AddWriteSet(addr, accesslist.BALANCE)
	}
	return fs.StateDb.Selfdestruct(addr)
}

func (fs *StateWithRwSets) Selfdestruct6780(addr common.Address) {
	fs.StateDb.Selfdestruct6780(addr)
}

// ----------------------Functional Methods---------------------
func (fs *StateWithRwSets) AddRefund(gas uint64) {
	fs.StateDb.AddRefund(gas)
}

func (fs *StateWithRwSets) SubRefund(gas uint64) {
	fs.StateDb.SubRefund(gas)
}

// AddAddressToAccessList adds the given address to the access list
func (fs *StateWithRwSets) AddAddressToAccessList(addr common.Address) bool {
	return fs.StateDb.AddAddressToAccessList(addr)
}

// AddSlotToAccessList adds the given (address, slot)-tuple to the access list
func (fs *StateWithRwSets) AddSlotToAccessList(addr common.Address, slot common.Hash) (bool, bool) {
	return fs.StateDb.AddSlotToAccessList(addr, slot)
}

// SlotInAccessList returns true if the given (address, slot)-tuple is in the access list.
func (fs *StateWithRwSets) SlotInAccessList(addr common.Address, slot common.Hash) (addressPresent bool, slotPresent bool) {
	return fs.StateDb.SlotInAccessList(addr, slot)
}

func (fs *StateWithRwSets) RevertToSnapshot(revid int) {
	fs.StateDb.RevertToSnapshot(revid)
}

func (fs *StateWithRwSets) Snapshot() int {
	return fs.StateDb.Snapshot()
}

func (fs *StateWithRwSets) AddLog(log *types.Log) {
	fs.StateDb.AddLog(log)
}

// func (fs *StateWithRwSets) AddPreimage(hash common.Hash, preimage []byte) {
// 	fs.StateDb.AddPreimage(hash, preimage)
// }

func (fs *StateWithRwSets) Prepare(rules *chain.Rules, sender, coinbase common.Address, dst *common.Address, precompiles []common.Address, list types2.AccessList) {
	fs.StateDb.Prepare(rules, sender, coinbase, dst, precompiles, list)
}

func (fs *StateWithRwSets) AddressInAccessList(addr common.Address) bool {
	return fs.StateDb.AddressInAccessList(addr)
}

func (fs *StateWithRwSets) SetTxContext(thash, bhash common.Hash, ti int) {
	fs.StateDb.SetTxContext(thash, bhash, ti)
}
