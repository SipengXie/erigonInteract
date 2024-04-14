package gria

import (
	"sync"
)

type Status int

const (
	Pending Status = iota
	Aborted
	Committed
)

type Version struct {
	Data   interface{}
	Tid    int
	Status Status
	// Readby cannot be accessed concurrently, only txs in the same group can do the read
	Readby    map[int]struct{}
	MaxReadby int

	Next  *Version
	Prev  *Version
	Plock sync.Mutex
	Nlock sync.Mutex
}

func NewVersion(data interface{}, tid int, status Status) *Version {
	return &Version{
		Data:      data,
		Tid:       tid,
		Status:    status,
		Readby:    make(map[int]struct{}),
		MaxReadby: -1,
		Next:      nil,
		Prev:      nil,
		Plock:     sync.Mutex{},
		Nlock:     sync.Mutex{},
	}
}

func (v *Version) InsertOrNext(iv *Version) *Version {
	v.Nlock.Lock()
	defer v.Nlock.Unlock()
	if v.Next == nil || v.updatePrev(iv) {
		iv.Next = v.Next
		v.Next = iv
		iv.Prev = v
		return nil
	} else {
		return v.Next
	}
}

func (v *Version) updatePrev(iv *Version) bool {
	v.Plock.Lock()
	defer v.Plock.Unlock()
	if iv.Tid < v.Tid {
		v.Prev = iv
		return true
	}
	return false
}

type VersionChain struct {
	Head *Version
}

func NewVersionChain() *VersionChain {
	return &VersionChain{
		Head: NewVersion(nil, -1, Committed), // an dummy head which means its from the stateSnapshot
	}
}

func (vc *VersionChain) InstallVersion(iv *Version) {
	cur_v := vc.Head
	for {
		if cur_v == nil {
			break
		}
		cur_v = cur_v.InsertOrNext(iv)
	}
}
