package state

import (
	"erigonInteract/gria"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
)

// after the Tx execution, insert the versions into the global version chain
// here if curVersion == localWrite, then we skip it.
type globalVersionChain struct {
	gvcBalance  sync.Map // global view Balance: version chain per record: addr -> *VersionChain
	gvcNonce    sync.Map // global view Nonce: version chain per record: addr -> *VersionChain
	gvcStorage  sync.Map // global view Storage: version chain per record: addr -> *sync.Map (hash -> *VersionChain)
	gvcCode     sync.Map // global view Code: version chain per record: addr -> *VersionChain
	gvdCodeHash sync.Map // global view CodeHash: version chain per record: addr -> *VersionChain
	gvcAlive    sync.Map // global view Alive: version chain per record: addr -> *VersionChain
}

func NewGlobalVersionChain() *globalVersionChain {
	return &globalVersionChain{
		gvcBalance:  sync.Map{},
		gvcNonce:    sync.Map{},
		gvcStorage:  sync.Map{},
		gvcCode:     sync.Map{},
		gvdCodeHash: sync.Map{},
		gvcAlive:    sync.Map{},
	}
}

// ------------------- insert version -------------------

func (gvc *globalVersionChain) insertBalanceVersion(addr common.Address, iv *gria.Version) {
	vc, _ := gvc.gvcBalance.LoadOrStore(addr, gria.NewVersionChain())
	vc.(*gria.VersionChain).InstallVersion(iv)
}

func (gvc *globalVersionChain) insertNonceVersion(addr common.Address, iv *gria.Version) {
	vc, _ := gvc.gvcNonce.LoadOrStore(addr, gria.NewVersionChain())
	vc.(*gria.VersionChain).InstallVersion(iv)
}

func (gvc *globalVersionChain) insertStorageVersion(addr common.Address, hash common.Hash, iv *gria.Version) {
	cache, _ := gvc.gvcStorage.LoadOrStore(addr, &sync.Map{})
	vc, _ := cache.(*sync.Map).LoadOrStore(hash, gria.NewVersionChain())
	vc.(*gria.VersionChain).InstallVersion(iv)
}

func (gvc *globalVersionChain) insertCodeVersion(addr common.Address, iv *gria.Version) {
	vc, _ := gvc.gvcCode.LoadOrStore(addr, gria.NewVersionChain())
	vc.(*gria.VersionChain).InstallVersion(iv)
}

func (gvc *globalVersionChain) insertCodeHashVersion(addr common.Address, iv *gria.Version) {
	vc, _ := gvc.gvdCodeHash.LoadOrStore(addr, gria.NewVersionChain())
	vc.(*gria.VersionChain).InstallVersion(iv)
}

func (gvc *globalVersionChain) insertAliveVersion(addr common.Address, iv *gria.Version) {
	vc, _ := gvc.gvcAlive.LoadOrStore(addr, gria.NewVersionChain())
	vc.(*gria.VersionChain).InstallVersion(iv)
}

// -------------------- get head version --------------------
func (gvc *globalVersionChain) getBalanceHead(addr common.Address) *gria.Version {
	vc, _ := gvc.gvcBalance.LoadOrStore(addr, gria.NewVersionChain())
	return vc.(*gria.VersionChain).Head
}

func (gvc *globalVersionChain) getNonceHead(addr common.Address) *gria.Version {
	vc, _ := gvc.gvcNonce.LoadOrStore(addr, gria.NewVersionChain())
	return vc.(*gria.VersionChain).Head
}

func (gvc *globalVersionChain) getStorageHead(addr common.Address, hash common.Hash) *gria.Version {
	cache, _ := gvc.gvcStorage.LoadOrStore(addr, &sync.Map{})
	vc, _ := cache.(*sync.Map).LoadOrStore(hash, gria.NewVersionChain())
	return vc.(*gria.VersionChain).Head
}

func (gvc *globalVersionChain) getCodeHead(addr common.Address) *gria.Version {
	vc, _ := gvc.gvcCode.LoadOrStore(addr, gria.NewVersionChain())
	return vc.(*gria.VersionChain).Head
}

func (gvc *globalVersionChain) getCodeHashHead(addr common.Address) *gria.Version {
	vc, _ := gvc.gvdCodeHash.LoadOrStore(addr, gria.NewVersionChain())
	return vc.(*gria.VersionChain).Head
}

func (gvc *globalVersionChain) getAliveHead(addr common.Address) *gria.Version {
	vc, _ := gvc.gvcAlive.LoadOrStore(addr, gria.NewVersionChain())
	return vc.(*gria.VersionChain).Head
}
