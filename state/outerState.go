package state

import (
	"erigonInteract/accesslist"
	"fmt"
	"math/big"
	"sync"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
)

// 整合gvc和scatterstate的外层state，用于Gria后的执行
type OuterState struct {
	gvc *globalVersionChain // global view: version chain per record
	sdb *ScatterState       // local view: scatter state

	Balances   sync.Map // addr -> *uint256.Int
	Nonces     sync.Map // addr -> uint64
	Codes      sync.Map // addr -> []byte
	CodeHashes sync.Map // addr -> common.Hash
	Alive      sync.Map // addr -> bool

	// stores pointers to another sync.Map
	Storages sync.Map // addr -> *sync.Map (hash -> hash)
}

func NewOuterState(gvc *globalVersionChain, sdb *ScatterState) *OuterState {
	return &OuterState{
		gvc:        gvc,
		sdb:        sdb,
		Balances:   sync.Map{},
		Nonces:     sync.Map{},
		Codes:      sync.Map{},
		CodeHashes: sync.Map{},
		Storages:   sync.Map{},
	}
}

func (os *OuterState) CreateAccount(addr common.Address, contractCreation bool) {
	balance := new(uint256.Int).SetUint64(0)
	nonce := uint64(0)
	code := []byte{}
	codeHash := common.Hash{}
	storage := new(sync.Map)

	os.Balances.Store(addr, balance)
	os.Nonces.Store(addr, nonce)
	os.Codes.Store(addr, code)
	os.CodeHashes.Store(addr, codeHash)
	os.Alive.Store(addr, true)
	os.Storages.Store(addr, storage)
}

func (os *OuterState) SubBalance(addr common.Address, value *uint256.Int) {
	data := os.GetBalance(addr)
	balance := new(uint256.Int).Sub(data, value)
	os.Balances.Store(addr, balance)
}

func (os *OuterState) AddBalance(addr common.Address, value *uint256.Int) {

	data := os.GetBalance(addr)

	balance := new(uint256.Int).Add(data, value)
	os.Balances.Store(addr, balance)
}

func (os *OuterState) GetBalance(addr common.Address) *uint256.Int {
	// 先从本地取
	data, exists := os.Balances.Load(addr)
	if exists {
		return data.(*uint256.Int)
	}
	if !exists {
		// 尝试从gvc取，如果取不到再从scatter取
		vc := os.gvc.getBalanceHead(addr)
		if vc != nil {
			if vc.Data != nil {
				data = vc.Data
				return data.(*uint256.Int)
			}
		} else {
			data, exists = os.sdb.Balances.Load(addr)
			if !exists { // 若这一步还未取到则返回0
				return uint256.NewInt(0)
			}
			return data.(*uint256.Int)
		}
	}
	return uint256.NewInt(0)
}

func (os *OuterState) GetNonce(addr common.Address) uint64 {
	// 先从本地取
	nonce, exists := os.Nonces.Load(addr)
	if exists {
		return nonce.(uint64)

	}
	if !exists {
		// 尝试从gvc取，如果取不到再从scatter取
		vc := os.gvc.getNonceHead(addr)
		if vc != nil {
			if vc.Data != nil {
				nonce = vc.Data
				return nonce.(uint64)
			}
		} else {
			nonce, exists = os.sdb.Nonces.Load(addr)
			if !exists { // 若这一步还未取到则返回0
				return 0
			}
		}
	}
	return 0
}

func (os *OuterState) SetNonce(addr common.Address, nonce uint64) {
	os.Nonces.Store(addr, nonce)
}

func (os *OuterState) GetCodeHash(addr common.Address) common.Hash {
	// 先从本地取
	codeHash, exists := os.CodeHashes.Load(addr)
	if exists {
		return codeHash.(common.Hash)
	}

	if !exists {
		// 尝试从gvc取，如果取不到再从scatter取
		vc := os.gvc.getCodeHashHead(addr)
		if vc != nil {
			if vc.Data != nil {
				codeHash = vc.Data
				return codeHash.(common.Hash)
			}
		} else {
			codeHash, exists = os.sdb.CodeHashes.Load(addr)
			if !exists { // 若这一步还未取到则返回0
				return common.Hash{}
			}
			return codeHash.(common.Hash)
		}
	}

	return common.Hash{}
}

func (os *OuterState) GetCode(addr common.Address) []byte {
	// 先从本地取
	code, exists := os.Codes.Load(addr)
	if exists {
		return code.([]byte)
	}

	if !exists {
		// 尝试从gvc取，如果取不到再从scatter取
		vc := os.gvc.getCodeHead(addr)
		if vc != nil {
			if vc.Data != nil {
				code = vc.Data
				return code.([]byte)
			}
		} else {
			code, exists = os.sdb.Codes.Load(addr)
			if !exists { // 若这一步还未取到则返回0
				return []byte{}
			}
			return code.([]byte)
		}
	}

	return []byte{}
}

func (os *OuterState) SetCode(addr common.Address, code []byte) {
	os.Codes.Store(addr, code)
	os.CodeHashes.Store(addr, crypto.Keccak256Hash(code))
}

func (os *OuterState) GetCodeSize(addr common.Address) int {
	code := os.GetCode(addr)
	return len(code)
}

func (os *OuterState) AddRefund(_ uint64) {
	// SKIP
}

func (os *OuterState) SubRefund(_ uint64) {
	// SKIP
}

func (os *OuterState) GetRefund() uint64 {
	// SKIP
	return 0
}

func (os *OuterState) GetCommittedState(addr common.Address, key *common.Hash, value *uint256.Int) {
	os.GetState(addr, key, value)
}

func (os *OuterState) GetState(addr common.Address, key *common.Hash, value *uint256.Int) {
	// 本地取状态（本地缓存）
	state, exists := os.Storages.Load(addr)
	if exists {
		storage := state.(*sync.Map)
		res, exists1 := storage.Load(*key)
		if exists1 {
			*value = res.(uint256.Int)
			return
		}
	}
	if !exists {
		vc := os.gvc.getStorageHead(addr, *key)
		if vc != nil {
			if vc.Data != nil {
				state = vc.Data.(uint256.Int)
				*value = state.(uint256.Int)
				return
			}
		} else {
			state, exists = os.sdb.Storages.Load(addr)
			if !exists { // 若这一步还未取到则返回0
				value.Clear()
				return
			}
			storage := state.(*sync.Map)
			res, exists1 := storage.Load(*key)
			if exists1 {
				*value = res.(uint256.Int)
				return
			}
		}
	}
	// 取不到从gvc取（一层缓存）
}

func (os *OuterState) SetState(addr common.Address, key *common.Hash, value uint256.Int) {
	state, exists := os.Storages.Load(addr)
	if !exists {
		storage := new(sync.Map)
		storage.Store(*key, value)
		os.Storages.Store(addr, storage)
		return
	}
	storage := state.(*sync.Map)
	storage.Store(*key, value)
}

func (os *OuterState) GetTransientState(addr common.Address, key common.Hash) uint256.Int {
	value := uint256.NewInt(0)
	os.GetState(addr, &key, value)
	return *value
}

func (os *OuterState) SetTransientState(addr common.Address, key common.Hash, value uint256.Int) {
	os.SetState(addr, &key, value)
}

func (os *OuterState) Selfdestruct(addr common.Address) bool {
	os.Balances.Store(addr, new(big.Int).SetUint64(0))
	os.Alive.Store(addr, false)
	return true
}

func (os *OuterState) HasSelfdestructed(addr common.Address) bool {
	alive, _ := os.Alive.Load(addr)
	return !alive.(bool)
}

func (os *OuterState) Selfdestruct6780(addr common.Address) {
	os.Selfdestruct(addr)
}

// Exist reports whether the given account exists in state.
// Notably this should also return true for self-destructed accounts.
func (os *OuterState) Exist(addr common.Address) bool {
	_, exists := os.Balances.Load(addr)
	return exists
}

// Empty returns whether the given account is empty. Empty
// is defined according to EIP161 (balance = nonce = code = 0).
func (os *OuterState) Empty(addr common.Address) bool {
	balance, exists := os.Balances.Load(addr)
	if !exists {
		return true
	}
	nonce, exists := os.Nonces.Load(addr)
	if !exists {
		return true
	}
	code, exists := os.Codes.Load(addr)
	if !exists {
		return true
	}
	if balance.(*uint256.Int).Sign() == 0 && nonce.(uint64) == 0 && len(code.([]byte)) == 0 {
		return true
	}
	return false
}

func (os *OuterState) AddressInAccessList(addr common.Address) bool {
	return true
}

func (os *OuterState) SlotInAccessList(addr common.Address, slot common.Hash) (addressOk bool, slotOk bool) {
	return true, true
}

// AddAddressToAccessList adds the given address to the access list. This operation is safe to perform
// even if the feature/fork is not active yet
func (os *OuterState) AddAddressToAccessList(addr common.Address) bool {
	// SKIP
	return false
}

// AddSlotToAccessList adds the given (address,slot) to the access list. This operation is safe to perform
// even if the feature/fork is not active yet
func (os *OuterState) AddSlotToAccessList(addr common.Address, slot common.Hash) (bool, bool) {
	// SKIP
	return false, false
}

func (os *OuterState) Prepare(rules *chain.Rules, sender common.Address, coinbase common.Address, dest *common.Address, precompiles []common.Address, txAccesses types2.AccessList) {
	// SKIP
}

func (os *OuterState) RevertToSnapshot(_ int) {
	// SKIP
}

func (os *OuterState) Snapshot() int {
	return 0
}

func (os *OuterState) AddLog(_ *types.Log) {
	// SKIP
}

func (os *OuterState) AddPreimage(_ common.Hash, _ []byte) {
	// SKIP
}

func (os *OuterState) SetBalance(addr common.Address, value *uint256.Int) {
	newVal := new(uint256.Int).Set(value)
	os.Balances.Store(addr, newVal)
}

func (os *OuterState) SetTxContext(_ common.Hash, _ int) {
	// SKIP
}

func (os *OuterState) Prefetch(statedb evmtypes.IntraBlockState, rwSets accesslist.RWSetList) {

	for _, rwSet := range rwSets {
		if rwSet == nil {
			return
		}
		for addr, State := range rwSet.ReadSet {
			for hash := range State {
				os.prefetch(addr, hash, statedb)
			}
		}
		for addr, State := range rwSet.WriteSet {
			if rwSet.WriteSet == nil {
				break
			}
			for hash := range State {
				os.prefetch(addr, hash, statedb)
			}
		}
	}
}

func (os *OuterState) prefetch(addr common.Address, hash common.Hash, statedb evmtypes.IntraBlockState) {
	if !os.Exist(addr) {
		os.CreateAccount(addr, true)
	}
	switch hash {
	case accesslist.BALANCE:
		balance := statedb.GetBalance(addr)
		os.SetBalance(addr, balance)
	case accesslist.NONCE:
		os.SetNonce(addr, statedb.GetNonce(addr))
	case accesslist.CODEHASH:
		// s.SetCodeHash(addr, statedb.GetCodeHash(addr))
	case accesslist.CODE:
		os.SetCode(addr, statedb.GetCode(addr))
	case accesslist.ALIVE:
		os.Alive.Store(addr, statedb.Exist(addr))
	default:
		value := uint256.NewInt(0)
		statedb.GetState(addr, &hash, value)
		os.SetState(addr, &hash, *value)
	}
}

func (os *OuterState) Equal(statedb evmtypes.IntraBlockState, rwSets accesslist.RWSetList) {
	for _, rwSet := range rwSets {
		for addr, State := range rwSet.ReadSet {
			for hash := range State {
				os.equal(addr, hash, statedb)
			}
		}
		for addr, State := range rwSet.WriteSet {
			for hash := range State {
				os.equal(addr, hash, statedb)
			}
		}
	}
}

func (os *OuterState) equal(addr common.Address, hash common.Hash, statedb evmtypes.IntraBlockState) {
	switch hash {
	case accesslist.BALANCE:
		balance := statedb.GetBalance(addr)
		if !os.GetBalance(addr).Eq(balance) {
			panic(fmt.Sprintf("Balance mismatch: %s %s %s", addr.Hex(), os.GetBalance(addr).String(), balance.String()))
		}
	case accesslist.NONCE:
		nonce := statedb.GetNonce(addr)
		if os.GetNonce(addr) != nonce {
			panic(fmt.Sprintf("Nonce mismatch: %s %d %d", addr.Hex(), os.GetNonce(addr), nonce))
		}
	case accesslist.CODEHASH:
		codeHash := statedb.GetCodeHash(addr)
		if os.GetCodeHash(addr) != codeHash {
			panic(fmt.Sprintf("CodeHash mismatch: %s %s %s", addr.Hex(), os.GetCodeHash(addr).Hex(), codeHash.Hex()))
		}
	case accesslist.CODE:
		code := statedb.GetCode(addr)
		if string(os.GetCode(addr)) != string(code) {
			panic(fmt.Sprintf("Code mismatch: %s %s %s", addr.Hex(), os.GetCode(addr), code))
		}
	case accesslist.ALIVE:
		isDead := statedb.HasSelfdestructed(addr)
		if os.HasSelfdestructed(addr) != isDead {
			panic(fmt.Sprintf("Alive mismatch: %s %t %t", addr.Hex(), !os.HasSelfdestructed(addr), isDead))
		}
	default:
		value := uint256.NewInt(0)
		statedb.GetState(addr, &hash, value)
		value2 := uint256.NewInt(0)
		os.GetState(addr, &hash, value2)
		if !value.Eq(value2) {
			panic(fmt.Sprintf("State mismatch: %s %s %s", addr.Hex(), value.String(), value2.String()))
		}
	}
}
