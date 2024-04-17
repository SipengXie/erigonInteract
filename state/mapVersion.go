package state

import (
	"erigonInteract/gria"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
)

type cacheVerison map[common.Hash]*gria.Version

const MAXINT = 1<<31 - 1

// versions are inserted into global version chain after the tx execution, MapVersion is just a simple record of the versionrent version
// only have getters and setters, will not be accessed conversionrently
type MapVersion struct {
	gvc             *globalVersionChain
	versionBalance  map[common.Address]*gria.Version //group view Balance: versionrent version per record: addr -> *Version
	versionNonce    map[common.Address]*gria.Version //group view Nonce: versionrent version per record: addr -> *Version
	versionStorage  map[common.Address]cacheVerison  //group view Storage: versionrent version per record: addr ->  map(hash -> *Version)
	versionCode     map[common.Address]*gria.Version //group view Code: versionrent version per record: addr -> *Version
	versionCodeHash map[common.Address]*gria.Version //group view CodeHash: versionrent version per record: addr -> *Version
	versionAlive    map[common.Address]*gria.Version //group view Alive: versionrent version per record: addr -> *Version
}

func newMapVersion(gvc *globalVersionChain) *MapVersion {
	return &MapVersion{
		gvc:             gvc,
		versionBalance:  make(map[common.Address]*gria.Version),
		versionNonce:    make(map[common.Address]*gria.Version),
		versionStorage:  make(map[common.Address]cacheVerison),
		versionCode:     make(map[common.Address]*gria.Version),
		versionCodeHash: make(map[common.Address]*gria.Version),
		versionAlive:    make(map[common.Address]*gria.Version),
	}
}

// ------------------ Getters -----------------------
// get the head version notifying this is from the stateSnapshot
func (c *MapVersion) getBalance(addr common.Address) *gria.Version {
	version, ok := c.versionBalance[addr]
	if !ok {
		c.versionBalance[addr] = c.gvc.getBalanceHead(addr)
		return c.versionBalance[addr]
	}
	return version
}

func (c *MapVersion) getNonce(addr common.Address) *gria.Version {
	version, ok := c.versionNonce[addr]
	if !ok {
		c.versionNonce[addr] = c.gvc.getNonceHead(addr)
		return c.versionNonce[addr]
	}
	return version
}

func (c *MapVersion) getStorage(addr common.Address, hash common.Hash) *gria.Version {
	cache, ok := c.versionStorage[addr]
	if !ok {
		cache = make(cacheVerison)
		c.versionStorage[addr] = cache
	}
	v, ok := cache[hash]
	if !ok {
		cache[hash] = c.gvc.getStorageHead(addr, hash)
		return cache[hash]
	}
	return v
}

func (c *MapVersion) getCode(addr common.Address) *gria.Version {
	version, ok := c.versionCode[addr]
	if !ok {
		c.versionCode[addr] = c.gvc.getCodeHead(addr)
		return c.versionCode[addr]
	}
	return version
}

func (c *MapVersion) getCodeHash(addr common.Address) *gria.Version {
	version, ok := c.versionCodeHash[addr]
	if !ok {
		c.versionCodeHash[addr] = c.gvc.getCodeHashHead(addr)
		return c.versionCodeHash[addr]
	}
	return version
}

func (c *MapVersion) getAlive(addr common.Address) *gria.Version {
	version, ok := c.versionAlive[addr]
	if !ok {
		c.versionAlive[addr] = c.gvc.getAliveHead(addr)
		return c.versionAlive[addr]
	}
	return version
}

// ------------------ Setters -----------------------

func (c *MapVersion) setBalance(addr common.Address, v *gria.Version) {
	c.versionBalance[addr] = v
}

func (c *MapVersion) setNonce(addr common.Address, v *gria.Version) {
	c.versionNonce[addr] = v
}

func (c *MapVersion) setStorage(addr common.Address, hash common.Hash, v *gria.Version) {
	if _, ok := c.versionStorage[addr]; !ok {
		c.versionStorage[addr] = make(cacheVerison)
	}
	c.versionStorage[addr][hash] = v
}

func (c *MapVersion) setCode(addr common.Address, v *gria.Version) {
	c.versionCode[addr] = v
}

func (c *MapVersion) setCodeHash(addr common.Address, v *gria.Version) {
	c.versionCodeHash[addr] = v
}

func (c *MapVersion) setAlive(addr common.Address, v *gria.Version) {
	c.versionAlive[addr] = v
}

// ------------------ ScanRead: return maxCur and minNext, used for reordering --------------------
func (c *MapVersion) ScanRead() (int, int) {
	var wait sync.WaitGroup
	var maxCurArray [6]int = [6]int{-1, -1, -1, -1, -1, -1}
	var minNextArray [6]int = [6]int{MAXINT, MAXINT, MAXINT, MAXINT, MAXINT, MAXINT}
	wait.Add(6)
	go c.scanReadBalance(&wait, &maxCurArray[0], &minNextArray[0])
	go c.scanReadNonce(&wait, &maxCurArray[1], &minNextArray[1])
	go c.scanReadStorage(&wait, &maxCurArray[2], &minNextArray[2])
	go c.scanReadCode(&wait, &maxCurArray[3], &minNextArray[3])
	go c.scanReadCodeHash(&wait, &maxCurArray[4], &minNextArray[4])
	go c.scanReadAlive(&wait, &maxCurArray[5], &minNextArray[5])
	wait.Wait()
	maxCur := maxCurArray[0]
	minNext := minNextArray[0]
	for i := range maxCurArray {
		if maxCurArray[i] > maxCur {
			maxCur = maxCurArray[i]
		}
		if minNextArray[i] < minNext {
			minNext = minNextArray[i]
		}
	}
	return maxCur, minNext
}

func (c *MapVersion) scanReadBalance(wait *sync.WaitGroup, maxCur *int, minNext *int) {
	defer wait.Done()
	for _, v := range c.versionBalance {
		if v.Tid > *maxCur {
			*maxCur = v.Tid
		}
		if v.Next != nil && v.Next.Tid < *minNext {
			*minNext = v.Next.Tid
		}
	}
}

func (c *MapVersion) scanReadNonce(wait *sync.WaitGroup, maxCur *int, minNext *int) {
	defer wait.Done()
	for _, v := range c.versionNonce {
		if v.Tid > *maxCur {
			*maxCur = v.Tid
		}
		if v.Next != nil && v.Next.Tid < *minNext {
			*minNext = v.Next.Tid
		}
	}
}

func (c *MapVersion) scanReadStorage(wait *sync.WaitGroup, maxCur *int, minNext *int) {
	defer wait.Done()
	for _, cache := range c.versionStorage {
		for _, v := range cache {
			if v.Tid > *maxCur {
				*maxCur = v.Tid
			}
			if v.Next != nil && v.Next.Tid < *minNext {
				*minNext = v.Next.Tid
			}
		}
	}
}

func (c *MapVersion) scanReadCode(wait *sync.WaitGroup, maxCur *int, minNext *int) {
	defer wait.Done()
	for _, v := range c.versionCode {
		if v.Tid > *maxCur {
			*maxCur = v.Tid
		}
		if v.Next != nil && v.Next.Tid < *minNext {
			*minNext = v.Next.Tid
		}
	}
}

func (c *MapVersion) scanReadCodeHash(wait *sync.WaitGroup, maxCur *int, minNext *int) {
	defer wait.Done()
	for _, v := range c.versionCodeHash {
		if v.Tid > *maxCur {
			*maxCur = v.Tid
		}
		if v.Next != nil && v.Next.Tid < *minNext {
			*minNext = v.Next.Tid
		}
	}
}

func (c *MapVersion) scanReadAlive(wait *sync.WaitGroup, maxCur *int, minNext *int) {
	defer wait.Done()
	for _, v := range c.versionAlive {
		if v.Tid > *maxCur {
			*maxCur = v.Tid
		}
		if v.Next != nil && v.Next.Tid < *minNext {
			*minNext = v.Next.Tid
		}
	}
}

// ------------------ ScanWrite: return max(maxPre, maxReadBy), used for reordering --------------------
func (c *MapVersion) ScanWrite() int {
	var wait sync.WaitGroup
	var resArray [6]int = [6]int{-1, -1, -1, -1, -1, -1}
	wait.Add(6)
	go c.scanWriteBalance(&wait, &resArray[0])
	go c.scanWriteNonce(&wait, &resArray[1])
	go c.scanWriteStorage(&wait, &resArray[2])
	go c.scanWriteCode(&wait, &resArray[3])
	go c.scanWriteCodeHash(&wait, &resArray[4])
	go c.scanWriteAlive(&wait, &resArray[5])
	wait.Wait()
	ans := resArray[0]
	for _, v := range resArray {
		if v > ans {
			ans = v
		}
	}
	return ans
}

func (c *MapVersion) scanWriteBalance(wait *sync.WaitGroup, res *int) {
	defer wait.Done()
	for _, v := range c.versionBalance {
		if v.Prev != nil && (v.Prev.Tid > *res || v.Prev.MaxReadby > *res) {
			if v.Prev.Tid > v.Prev.MaxReadby {
				*res = v.Prev.Tid
			} else {
				*res = v.Prev.MaxReadby
			}
		}
	}
}

func (c *MapVersion) scanWriteNonce(wait *sync.WaitGroup, res *int) {
	defer wait.Done()
	for _, v := range c.versionNonce {
		if v.Prev != nil && (v.Prev.Tid > *res || v.Prev.MaxReadby > *res) {
			if v.Prev.Tid > v.Prev.MaxReadby {
				*res = v.Prev.Tid
			} else {
				*res = v.Prev.MaxReadby
			}
		}
	}
}
func (c *MapVersion) scanWriteStorage(wait *sync.WaitGroup, res *int) {
	defer wait.Done()
	for _, cache := range c.versionStorage {
		for _, v := range cache {
			if v.Prev != nil && (v.Prev.Tid > *res || v.Prev.MaxReadby > *res) {
				if v.Prev.Tid > v.Prev.MaxReadby {
					*res = v.Prev.Tid
				} else {
					*res = v.Prev.MaxReadby
				}
			}
		}
	}
}

func (c *MapVersion) scanWriteCode(wait *sync.WaitGroup, res *int) {
	defer wait.Done()
	for _, v := range c.versionCode {
		if v.Prev != nil && (v.Prev.Tid > *res || v.Prev.MaxReadby > *res) {
			if v.Prev.Tid > v.Prev.MaxReadby {
				*res = v.Prev.Tid
			} else {
				*res = v.Prev.MaxReadby
			}
		}
	}
}

func (c *MapVersion) scanWriteCodeHash(wait *sync.WaitGroup, res *int) {
	defer wait.Done()
	for _, v := range c.versionCodeHash {
		if v.Prev != nil && (v.Prev.Tid > *res || v.Prev.MaxReadby > *res) {
			if v.Prev.Tid > v.Prev.MaxReadby {
				*res = v.Prev.Tid
			} else {
				*res = v.Prev.MaxReadby
			}
		}
	}
}

func (c *MapVersion) scanWriteAlive(wait *sync.WaitGroup, res *int) {
	defer wait.Done()
	for _, v := range c.versionAlive {
		if v.Prev != nil && (v.Prev.Tid > *res || v.Prev.MaxReadby > *res) {
			if v.Prev.Tid > v.Prev.MaxReadby {
				*res = v.Prev.Tid
			} else {
				*res = v.Prev.MaxReadby
			}
		}
	}
}

// ------------------ GetAllReadbys for commit, used for cascadeAborts ------------------------
func (c *MapVersion) GetAllReadbys() []int {
	var wait sync.WaitGroup
	var res = make([]map[int]struct{}, 6)
	for i := range res {
		res[i] = make(map[int]struct{})
	}
	wait.Add(6)
	go c.getAllReadBysBalance(&wait, &res[0])
	go c.getAllReadBysNonce(&wait, &res[1])
	go c.getAllReadBysStorage(&wait, &res[2])
	go c.getAllReadBysCode(&wait, &res[3])
	go c.getAllReadBysCodeHash(&wait, &res[4])
	go c.getAllReadBysAlive(&wait, &res[5])
	wait.Wait()
	ans := make(map[int]struct{})
	for _, v := range res {
		for tid := range v {
			ans[tid] = struct{}{}
		}
	}
	var ansArray []int
	for tid := range ans {
		ansArray = append(ansArray, tid)
	}
	return ansArray
}

func (c *MapVersion) getAllReadBysBalance(wait *sync.WaitGroup, res *map[int]struct{}) {
	defer wait.Done()
	for _, v := range c.versionBalance {
		for tid := range v.Readby {
			(*res)[tid] = struct{}{}
		}
	}
}

func (c *MapVersion) getAllReadBysNonce(wait *sync.WaitGroup, res *map[int]struct{}) {
	defer wait.Done()
	for _, v := range c.versionNonce {
		for tid := range v.Readby {
			(*res)[tid] = struct{}{}
		}
	}
}

func (c *MapVersion) getAllReadBysStorage(wait *sync.WaitGroup, res *map[int]struct{}) {
	defer wait.Done()
	for _, cache := range c.versionStorage {
		for _, v := range cache {
			for tid := range v.Readby {
				(*res)[tid] = struct{}{}
			}
		}
	}
}

func (c *MapVersion) getAllReadBysCode(wait *sync.WaitGroup, res *map[int]struct{}) {
	defer wait.Done()
	for _, v := range c.versionCode {
		for tid := range v.Readby {
			(*res)[tid] = struct{}{}
		}
	}
}

func (c *MapVersion) getAllReadBysCodeHash(wait *sync.WaitGroup, res *map[int]struct{}) {
	defer wait.Done()
	for _, v := range c.versionCodeHash {
		for tid := range v.Readby {
			(*res)[tid] = struct{}{}
		}
	}
}

func (c *MapVersion) getAllReadBysAlive(wait *sync.WaitGroup, res *map[int]struct{}) {
	defer wait.Done()
	for _, v := range c.versionAlive {
		for tid := range v.Readby {
			(*res)[tid] = struct{}{}
		}
	}
}

// --------------------------- GetReads for readsets, used for rechecking ---------------------------
func (c *MapVersion) GetReads() []*gria.Version {
	var wait sync.WaitGroup
	res := make([][]*gria.Version, 6)
	for i := range res {
		res[i] = make([]*gria.Version, 0)
	}
	wait.Add(6)
	go c.getReadsBalance(&wait, &res[0])
	go c.getReadsNonce(&wait, &res[1])
	go c.getReadsStorage(&wait, &res[2])
	go c.getReadsCode(&wait, &res[3])
	go c.getReadsCodeHash(&wait, &res[4])
	go c.getReadsAlive(&wait, &res[5])
	wait.Wait()
	ans := make([]*gria.Version, 0)
	for _, v := range res {
		ans = append(ans, v...)
	}
	return ans
}

func (c *MapVersion) getReadsBalance(wait *sync.WaitGroup, res *[]*gria.Version) {
	defer wait.Done()
	for _, v := range c.versionBalance {
		*res = append(*res, v)
	}
}

func (c *MapVersion) getReadsNonce(wait *sync.WaitGroup, res *[]*gria.Version) {
	defer wait.Done()
	for _, v := range c.versionNonce {
		*res = append(*res, v)
	}
}

func (c *MapVersion) getReadsStorage(wait *sync.WaitGroup, res *[]*gria.Version) {
	defer wait.Done()
	for _, cache := range c.versionStorage {
		for _, v := range cache {
			*res = append(*res, v)
		}
	}
}

func (c *MapVersion) getReadsCode(wait *sync.WaitGroup, res *[]*gria.Version) {
	defer wait.Done()
	for _, v := range c.versionCode {
		*res = append(*res, v)
	}
}

func (c *MapVersion) getReadsCodeHash(wait *sync.WaitGroup, res *[]*gria.Version) {
	defer wait.Done()
	for _, v := range c.versionCodeHash {
		*res = append(*res, v)
	}
}

func (c *MapVersion) getReadsAlive(wait *sync.WaitGroup, res *[]*gria.Version) {
	defer wait.Done()
	for _, v := range c.versionAlive {
		*res = append(*res, v)
	}
}

// ------------------ SetStatus for write set -----------------------
func (c *MapVersion) SetStatus(status gria.Status) {
	var wait sync.WaitGroup
	wait.Add(6)
	go c.setStatusBalance(&wait, status)
	go c.setStatusNonce(&wait, status)
	go c.setStatusStorage(&wait, status)
	go c.setStatusCode(&wait, status)
	go c.setStatusCodeHash(&wait, status)
	go c.setStatusAlive(&wait, status)
	wait.Wait()
}

func (c *MapVersion) setStatusBalance(wait *sync.WaitGroup, status gria.Status) {
	defer wait.Done()
	for _, v := range c.versionBalance {
		v.Status = status
	}
}

func (c *MapVersion) setStatusNonce(wait *sync.WaitGroup, status gria.Status) {
	defer wait.Done()
	for _, v := range c.versionNonce {
		v.Status = status
	}
}

func (c *MapVersion) setStatusStorage(wait *sync.WaitGroup, status gria.Status) {
	defer wait.Done()
	for _, cache := range c.versionStorage {
		for _, v := range cache {
			v.Status = status
		}
	}
}

func (c *MapVersion) setStatusCode(wait *sync.WaitGroup, status gria.Status) {
	defer wait.Done()
	for _, v := range c.versionCode {
		v.Status = status
	}
}

func (c *MapVersion) setStatusCodeHash(wait *sync.WaitGroup, status gria.Status) {
	defer wait.Done()
	for _, v := range c.versionCodeHash {
		v.Status = status
	}
}

func (c *MapVersion) setStatusAlive(wait *sync.WaitGroup, status gria.Status) {
	defer wait.Done()
	for _, v := range c.versionAlive {
		v.Status = status
	}
}
