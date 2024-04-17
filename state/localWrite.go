package state

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
)

type cache map[common.Hash]uint256.Int

// only need getter and setter
type LocalWrite struct {
	localBalance  map[common.Address]uint256.Int // Tx view localWrite for Balance
	localNonce    map[common.Address]uint64      // Tx view localWrite for Nonce
	localStorage  map[common.Address]cache       // Tx view localWrite for Storage
	localCode     map[common.Address][]byte      // Tx view localWrite for Code
	localCodeHash map[common.Address]common.Hash // Tx view localWrite for CodeHash
	localAlive    map[common.Address]bool        // Tx view localWrite for Alive
}

func newLocalWrite() *LocalWrite {
	return &LocalWrite{
		localBalance:  make(map[common.Address]uint256.Int),
		localNonce:    make(map[common.Address]uint64),
		localStorage:  make(map[common.Address]cache),
		localCode:     make(map[common.Address][]byte),
		localCodeHash: make(map[common.Address]common.Hash),
		localAlive:    make(map[common.Address]bool),
	}
}

// ------------------ Getters for localWrite -----------------------
func (l *LocalWrite) getBalance(addr common.Address) (*uint256.Int, bool) {
	balance, ok := l.localBalance[addr]
	return &balance, ok
}

func (l *LocalWrite) getNonce(addr common.Address) (uint64, bool) {
	nonce, ok := l.localNonce[addr]
	return nonce, ok
}

func (l *LocalWrite) getStorage(addr common.Address, hash common.Hash) (*uint256.Int, bool) {
	cache, ok := l.localStorage[addr]
	if !ok {
		return nil, ok
	}
	value, ok := cache[hash]
	return &value, ok
}

func (l *LocalWrite) getCode(addr common.Address) ([]byte, bool) {
	code, ok := l.localCode[addr]
	return code, ok
}

func (l *LocalWrite) getCodeHash(addr common.Address) (common.Hash, bool) {
	codeHash, ok := l.localCodeHash[addr]
	return codeHash, ok
}

func (l *LocalWrite) getAlive(addr common.Address) (bool, bool) {
	alive, ok := l.localAlive[addr]
	return alive, ok
}

// ------------------ Setters for localWrite -----------------------
func (l *LocalWrite) setBalance(addr common.Address, balance *uint256.Int) {
	b := l.localBalance[addr]
	b.Set(balance)
}

func (l *LocalWrite) setNonce(addr common.Address, nonce uint64) {
	l.localNonce[addr] = nonce
}

func (l *LocalWrite) setStorage(addr common.Address, hash common.Hash, value uint256.Int) {
	if _, ok := l.localStorage[addr]; !ok {
		l.localStorage[addr] = make(cache)
	}
	l.localStorage[addr][hash] = value
}

func (l *LocalWrite) setCode(addr common.Address, code []byte) {
	l.localCode[addr] = code
}

func (l *LocalWrite) setCodeHash(addr common.Address, codeHash common.Hash) {
	l.localCodeHash[addr] = codeHash
}

func (l *LocalWrite) setAlive(addr common.Address, alive bool) {
	l.localAlive[addr] = alive
}

// ------------------ Wrappers for localWrite -----------------------
func (l *LocalWrite) createAccount(addr common.Address) {
	l.setBalance(addr, uint256.NewInt(0))
	l.setNonce(addr, 0)
	l.setCode(addr, []byte{})
	l.setCodeHash(addr, common.Hash{})
	l.setAlive(addr, true)
	l.localStorage[addr] = make(cache)
}
