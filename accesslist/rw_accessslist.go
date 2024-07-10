package accesslist

import (
	"crypto/sha256"
	"encoding/json"
	"sort"

	"github.com/ledgerwatch/erigon-lib/common"
)

var (
	CODE     = common.Hash(sha256.Sum256([]byte("code")))
	CODEHASH = common.Hash(sha256.Sum256([]byte("codeHash")))
	BALANCE  = common.Hash(sha256.Sum256([]byte("balance")))
	NONCE    = common.Hash(sha256.Sum256([]byte("nonce")))
	ALIVE    = common.Hash(sha256.Sum256([]byte("alive")))
)

type State map[common.Hash]struct{}

type ALTuple map[common.Address]State

type RWSetList []*RWSet

func (tuple ALTuple) Add(addr common.Address, hash common.Hash) {
	if _, ok := tuple[addr]; !ok {
		tuple[addr] = make(State)
	}
	tuple[addr][hash] = struct{}{}
}

func (tuple ALTuple) Contains(addr common.Address, hash common.Hash) bool {
	if _, ok := tuple[addr]; !ok {
		return false
	}
	_, ok := tuple[addr][hash]
	return ok
}

type RWSet struct {
	ReadSet  ALTuple
	WriteSet ALTuple
}

func NewRWSet() *RWSet {
	return &RWSet{
		ReadSet:  make(ALTuple),
		WriteSet: make(ALTuple),
	}
}

func (RWSets RWSet) AddReadSet(addr common.Address, hash common.Hash) {
	RWSets.ReadSet.Add(addr, hash)
}

func (RWSets RWSet) AddWriteSet(addr common.Address, hash common.Hash) {
	RWSets.WriteSet.Add(addr, hash)
}

func (RWSets RWSet) HasConflict(other RWSet) bool {
	for addr, state := range RWSets.ReadSet {
		for hash := range state {
			if other.WriteSet.Contains(addr, hash) {
				return true
			}
		}
	}
	for addr, state := range RWSets.WriteSet {
		for hash := range state {
			if other.WriteSet.Contains(addr, hash) {
				return true
			}
			if other.ReadSet.Contains(addr, hash) {
				return true
			}
		}
	}
	return false
}

func (RWSets RWSet) Equal(other RWSet) bool {
	if len(RWSets.ReadSet) != len(other.ReadSet) {
		return false
	}
	if len(RWSets.WriteSet) != len(other.WriteSet) {
		return false
	}

	for addr, state := range RWSets.ReadSet {
		for hash := range state {
			if !other.ReadSet.Contains(addr, hash) {
				return false
			}
		}
	}

	for addr, state := range RWSets.WriteSet {
		for hash := range state {
			if !other.WriteSet.Contains(addr, hash) {
				return false
			}
		}
	}

	return true
}

func DecodeHash(hash common.Hash) string {
	switch hash {
	case CODE:
		return "code"
	case BALANCE:
		return "balance"
	case ALIVE:
		return "alive"
	case CODEHASH:
		return "codeHash"
	case NONCE:
		return "nonce"
	default:
		return hash.Hex()
	}
}

func encodeHash(str string) common.Hash {
	switch str {
	case "code":
		return CODE
	case "balance":
		return BALANCE
	case "alive":
		return ALIVE
	case "codeHash":
		return CODEHASH
	case "nonce":
		return NONCE
	default:
		return common.HexToHash(str)
	}
}

func (RWSets RWSet) ToJsonStruct() RWSetJson {
	readAL := make(map[common.Address][]string)
	writeAL := make(map[common.Address][]string)

	for addr, state := range RWSets.ReadSet {
		for hash := range state {
			readAL[addr] = append(readAL[addr], DecodeHash(hash))
		}
	}

	for addr, state := range RWSets.WriteSet {
		for hash := range state {
			writeAL[addr] = append(writeAL[addr], DecodeHash(hash))
		}
	}

	return RWSetJson{
		ReadSet:  readAL,
		WriteSet: writeAL,
	}
}

type RWSetJson struct {
	ReadSet  map[common.Address][]string `json:"readSet"`
	WriteSet map[common.Address][]string `json:"writeSet"`
}

func (rwj RWSetJson) ToString() string {
	b, _ := json.Marshal(rwj)
	return string(b)
}

// readBy / writeBy 所依赖的数据结构
type AccessedBy map[common.Address]map[common.Hash]map[uint]struct{}

func NewAccessedBy() AccessedBy {
	return make(map[common.Address]map[common.Hash]map[uint]struct{})
}

func (accessedBy AccessedBy) Add(addr common.Address, hash common.Hash, txID uint) {
	if _, ok := accessedBy[addr]; !ok {
		accessedBy[addr] = make(map[common.Hash]map[uint]struct{})
	}
	if _, ok := accessedBy[addr][hash]; !ok {
		accessedBy[addr][hash] = make(map[uint]struct{})
	}
	accessedBy[addr][hash][txID] = struct{}{}
}

// 从小到大返回一个记录被访问的txID的数组
func (accessedBy AccessedBy) TxIds(addr common.Address, hash common.Hash) []uint {
	txIds := make([]uint, 0)

	if _, ok := accessedBy[addr]; !ok {
		return txIds
	} else if _, ok := accessedBy[addr][hash]; !ok {
		return txIds
	} else {
		for txID := range accessedBy[addr][hash] {
			txIds = append(txIds, txID)
		}
	}

	sort.Slice(txIds, func(i, j int) bool {
		return txIds[i] < txIds[j]
	})
	return txIds
}

type RwAccessedBy struct {
	ReadBy  AccessedBy
	WriteBy AccessedBy
}

func NewRwAccessedBy() *RwAccessedBy {
	return &RwAccessedBy{
		ReadBy:  NewAccessedBy(),
		WriteBy: NewAccessedBy(),
	}
}

func (rw *RwAccessedBy) Add(set *RWSet, txId uint) {
	if set == nil {
		return
	}
	for addr, state := range set.ReadSet {
		for hash := range state {
			rw.ReadBy.Add(addr, hash, txId)
		}
	}
	for addr, state := range set.WriteSet {
		for hash := range state {
			rw.WriteBy.Add(addr, hash, txId)
		}
	}
}

func (rw *RwAccessedBy) Copy() *RwAccessedBy {
	newRw := NewRwAccessedBy()
	for addr, hashMap := range rw.ReadBy {
		for hash, txMap := range hashMap {
			for txId := range txMap {
				newRw.ReadBy.Add(addr, hash, txId)
			}
		}
	}
	for addr, hashMap := range rw.WriteBy {
		for hash, txMap := range hashMap {
			for txId := range txMap {
				newRw.WriteBy.Add(addr, hash, txId)
			}
		}
	}
	return newRw
}
