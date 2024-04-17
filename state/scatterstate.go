package state

import (
	"erigonInteract/accesslist"
	"math/big"
	"sync"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
)

// 不完整的逻辑，也没有存储storageRoots
type ScatterState struct {
	Balances   sync.Map // addr -> *uint256.Int
	Nonces     sync.Map // addr -> uint64
	Codes      sync.Map // addr -> []byte
	CodeHashes sync.Map // addr -> common.Hash
	Alive      sync.Map // addr -> bool

	// stores pointers to another sync.Map
	Storages sync.Map // addr -> *sync.Map (hash -> hash)
}

func NewScatterState() *ScatterState {
	return &ScatterState{
		Balances:   sync.Map{},
		Nonces:     sync.Map{},
		Codes:      sync.Map{},
		CodeHashes: sync.Map{},
		Storages:   sync.Map{},
	}
}

func (s *ScatterState) CreateAccount(addr common.Address, contractCreation bool) {
	balance := new(uint256.Int).SetUint64(0)
	nonce := uint64(0)
	code := []byte{}
	codeHash := common.Hash{}
	storage := new(sync.Map)

	s.Balances.Store(addr, balance)
	s.Nonces.Store(addr, nonce)
	s.Codes.Store(addr, code)
	s.CodeHashes.Store(addr, codeHash)
	s.Alive.Store(addr, true)
	s.Storages.Store(addr, storage)
}

func (s *ScatterState) SubBalance(addr common.Address, value *uint256.Int) {
	balance, exists := s.Balances.Load(addr)
	if !exists {
		return
	}
	balance = new(uint256.Int).Sub(balance.(*uint256.Int), value)
	s.Balances.Store(addr, balance)
}

func (s *ScatterState) AddBalance(addr common.Address, value *uint256.Int) {
	balance, exists := s.Balances.Load(addr)
	if !exists {
		return
	}
	balance = new(uint256.Int).Add(balance.(*uint256.Int), value)
	s.Balances.Store(addr, balance)
}

func (s *ScatterState) GetBalance(addr common.Address) *uint256.Int {
	balance, exists := s.Balances.Load(addr)
	if !exists {
		return u256.Num0
	}
	return balance.(*uint256.Int)
}

func (s *ScatterState) GetNonce(addr common.Address) uint64 {
	nonce, exists := s.Nonces.Load(addr)
	if !exists {
		return 0
	}
	return nonce.(uint64)
}

func (s *ScatterState) SetNonce(addr common.Address, nonce uint64) {
	s.Nonces.Store(addr, nonce)
}

func (s *ScatterState) GetCodeHash(addr common.Address) common.Hash {
	codeHash, exists := s.CodeHashes.Load(addr)
	if !exists {
		return common.Hash{}
	}
	return codeHash.(common.Hash)
}

func (s *ScatterState) GetCode(addr common.Address) []byte {
	code, exists := s.Codes.Load(addr)
	if !exists {
		return []byte{}
	}
	return code.([]byte)
}

func (s *ScatterState) SetCode(addr common.Address, code []byte) {
	s.Codes.Store(addr, code)
}

func (s *ScatterState) GetCodeSize(addr common.Address) int {
	code, exists := s.Codes.Load(addr)
	if !exists {
		return 0
	}
	return len(code.([]byte))
}

func (s *ScatterState) AddRefund(_ uint64) {
	// SKIP
}

func (s *ScatterState) SubRefund(_ uint64) {
	// SKIP
}

func (s *ScatterState) GetRefund() uint64 {
	// SKIP
	return 0
}

func (s *ScatterState) GetCommittedState(addr common.Address, key *common.Hash, value *uint256.Int) {
	s.GetState(addr, key, value)

}

func (s *ScatterState) GetState(addr common.Address, key *common.Hash, value *uint256.Int) {
	state, exists := s.Storages.Load(addr)
	if !exists {
		value.Clear()
		return
	}
	storage := state.(*sync.Map)
	res, exists := storage.Load(*key)
	if exists {
		*value = res.(uint256.Int)
	}
	value.Clear()
}

func (s *ScatterState) SetState(addr common.Address, key *common.Hash, value uint256.Int) {
	state, exists := s.Storages.Load(addr)
	if !exists {
		storage := new(sync.Map)
		storage.Store(*key, value)
		s.Storages.Store(addr, storage)
		return
	}
	storage := state.(*sync.Map)
	storage.Store(*key, value)
}

func (s *ScatterState) GetTransientState(addr common.Address, key common.Hash) uint256.Int {
	var value uint256.Int
	s.GetState(addr, &key, &value)
	return value
}

func (s *ScatterState) SetTransientState(addr common.Address, key common.Hash, value uint256.Int) {
	s.SetState(addr, &key, value)
}

func (s *ScatterState) Selfdestruct(addr common.Address) bool {
	s.Balances.Store(addr, new(big.Int).SetUint64(0))
	s.Alive.Store(addr, false)
	return true
}

func (s *ScatterState) HasSelfdestructed(addr common.Address) bool {
	alive, _ := s.Alive.Load(addr)
	return !alive.(bool)
}

func (s *ScatterState) Selfdestruct6780(addr common.Address) {
	s.Selfdestruct(addr)
}

// Exist reports whether the given account exists in state.
// Notably this should also return true for self-destructed accounts.
func (s *ScatterState) Exist(addr common.Address) bool {
	_, exists := s.Balances.Load(addr)
	return exists
}

// Empty returns whether the given account is empty. Empty
// is defined according to EIP161 (balance = nonce = code = 0).
func (s *ScatterState) Empty(addr common.Address) bool {
	balance, exists := s.Balances.Load(addr)
	if !exists {
		return true
	}
	nonce, exists := s.Nonces.Load(addr)
	if !exists {
		return true
	}
	code, exists := s.Codes.Load(addr)
	if !exists {
		return true
	}
	if balance.(*uint256.Int).Sign() == 0 && nonce.(uint64) == 0 && len(code.([]byte)) == 0 {
		return true
	}
	return false
}

func (s *ScatterState) AddressInAccessList(addr common.Address) bool {
	return false
}

func (s *ScatterState) SlotInAccessList(addr common.Address, slot common.Hash) (addressOk bool, slotOk bool) {
	return false, false
}

// AddAddressToAccessList adds the given address to the access list. This operation is safe to perform
// even if the feature/fork is not active yet
func (s *ScatterState) AddAddressToAccessList(addr common.Address) bool {
	// SKIP
	return true
}

// AddSlotToAccessList adds the given (address,slot) to the access list. This operation is safe to perform
// even if the feature/fork is not active yet
func (s *ScatterState) AddSlotToAccessList(addr common.Address, slot common.Hash) (bool, bool) {
	// SKIP
	return true, true
}

func (s *ScatterState) Prepare(rules *chain.Rules, sender common.Address, coinbase common.Address, dest *common.Address, precompiles []common.Address, txAccesses types2.AccessList) {
	// SKIP
}

func (s *ScatterState) RevertToSnapshot(_ int) {
	// SKIP
}

func (s *ScatterState) Snapshot() int {
	return 0
}

func (s *ScatterState) AddLog(_ *types.Log) {
	// SKIP
}

func (s *ScatterState) AddPreimage(_ common.Hash, _ []byte) {
	// SKIP
}

func (s *ScatterState) SetBalance(addr common.Address, value *uint256.Int) {
	s.Balances.Store(addr, value)
}

func (s *ScatterState) SetTxContext(_ common.Hash, _ int) {
	// SKIP
}

func (s *ScatterState) Prefetch(statedb evmtypes.IntraBlockState, rwSets accesslist.RWSetList) {
	for _, rwSet := range rwSets {
		for addr, State := range rwSet.ReadSet {
			for hash := range State {
				s.prefetch(addr, hash, statedb)
			}
		}
		for addr, State := range rwSet.WriteSet {
			for hash := range State {
				s.prefetch(addr, hash, statedb)
			}
		}
	}
}

func (s *ScatterState) prefetch(addr common.Address, hash common.Hash, statedb evmtypes.IntraBlockState) {
	if !s.Exist(addr) {
		s.CreateAccount(addr, true)
	}
	switch hash {
	case accesslist.BALANCE:
		s.SetBalance(addr, statedb.GetBalance(addr))
	case accesslist.NONCE:
		s.SetNonce(addr, statedb.GetNonce(addr))
	case accesslist.CODEHASH:
		// s.SetCodeHash(addr, statedb.GetCodeHash(addr))
	case accesslist.CODE:
		s.SetCode(addr, statedb.GetCode(addr))
	case accesslist.ALIVE:
		// s.SetAlive(addr, statedb.Exist(addr))
	default:
		var value uint256.Int
		statedb.GetState(addr, &hash, &value)
		s.SetState(addr, &hash, value)
	}
}
