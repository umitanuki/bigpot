package storage

import (
	"sync"

	"bigpot/system"
	"bigpot/system/spin"
)

type bufferTag struct {
	reln system.RelFileNode
	block system.BlockNumber
}

type bufferDesc struct {
	tag		bufferTag
	usageCount uint16
	refcount uint
	waitBackendid int
	headerSpin spin.Lock
	id	int
	freeNext int
	ioInProgressLock sync.RWMutex
	contentLock sync.RWMutex
}

type Buffer [system.BlockSize]byte

type SharedBufMgr struct {
	lookup map[tag]int
	lookupLock sync.RWMutex // to be partitioned
	descriptors []bufferDesc
	pool        []Buffer
}

type LocalBufMgr struct {
	shared *SharedBufMgr
	privateRefCount []int
}

func NewSharedBufferManager(nbuffers int) {
	mgr := &SharedBufMgr{
		lookup: map[tag]*bufferDesc{},
		descriptors: make([]bufferDesc, nbuffers),
		pool: make([]Buffer, nbuffers),
	}

	return mgr
}

func NewBufferManager(shared *BufMgr) {
	return &BufMgr{
		shared: shared,
		privateRefCount: make([]int, len(mgr.pool)),
	}
}

func (bufdesc *bufferDesc) lock() {
	bufdesc.headerSpin.Lock()
}

func (bufdesc *bufferDesc) unlock() {
	bufdesc.headerSpin.Unlock()
}

func (mgr *BufMgr) pinBuffer(bufdesc *bufferDesc) bool {
	bufId := bufdesc.id
	var result bool

	if mgr.privateRefCount[bufId] == 0 {
		bufdesc.Lock()
		bufdesc.refCount++
		// TODO: strategy
		// if strategy == default...

		result = (bufdesc.flags & bmValid) != 0
		bufdesc.Unlock()
	} else {
		// If we previously pinned the buffer, it must surely be valid
		result = true
	}
	mgr.privateRefCount[bufId]++
	// TODO: ResourceOwner?
	return result
}

func (mgr *BufMgr) pinLockedBuffer(bufdesc *bufferDesc) {
	bufId := bufdesc.id

	if mgr.privateRefCount[bufId] == 0 {
		bufdesc.refCount++
	}
	mgr.privateRefCount[bufId]++
	// TODO: ResourceOwner?
}

func (mgr *BufMgr) unpinBuffer(bufdesc *bufferDesc) {
	bufId := bufdesc.id

	mgr.privateRefCount[bufId]--
	if mgr.privateRefCount[bufId] == 0 {
		bufdesc.Lock()
		bufdesc.refCount--
		// TODO: bufdesc.flags & bmPinCountWaiter
		bufdesc.Unlock()
	}
}

func (mgr *BufMgr) bufferAlloc(reln system.RelFileNode, block system.BlockNumber) (*bufferDesc, bool, error) {
	tag := bufferTag{
		reln: reln,
		block: block,
	}

	mgr.lookupLock.RLock()
	if bufId, found := mgr.lookup[tag]; found {
		bufdesc := &mgr.descriptors[bufId]
		valid := mgr.pinBuffer(bufdesc)
		mgr.lookupLock.RUnlock()

		if !valid {
			if bufdesc.startIO(true) {
				found = false
			}
		}
		return bufdesc, found, nil
	}
	mgr.lookupLock.RUnlock()

	for {
		bufdesc, oldFlags := mgr.strategy.GetBuffer()

		if (oldFlags & bmDirty) != 0 {
			// TODO: LWLockConditionalAcquire
			// TODO: non-default strategy

			bufdesc.contentLock.RLock()
			mgr.FlushBuffer(bufdesc)
			bufdesc.contentLock.RUnlock()

		}

		// TODO: oldFlags & bmTagValid
		mgr.lookupLock.Lock()

		if bufId, found := mgr.lookup[tag]; found {
			mgr.unpinBuffer(bufdesc)
			bufdesc = &mgr.descriptors[bufId]
			valid := mgr.pinBuffer(bufdesc)

			if !valid {
				if bufdesc.startIO(true) {
					found = false
				}
			}
			return bufdesc, found, nil
		} else {
			mgr.lookup[tag] = bufdesc
		}

		bufdesc.lock()

		oldFlags = bufdesc.flags
		if bufdesc.refCount == 1 && (oldFlags & bmDirty) == 0 {
			break
		}

		bufdesc.unlock()
		delete(mgr.lookup, tag)
		mgr.lookupLock.Unlock()
		mgr.unpinBuffer(bufdesc)
	}

	bufdesc.tag = tag
	bufdesc.flags &= ~(bmValid | bmDirty | bmJustDirtied | bmCheckpointNeeded | bmIoErro | bmPermanent)
	// TODO: "permanent"
	bufdesc.usageCount = 1
	bufdesc.unlock()

	mgr.lookupLock.Unlock()

	found = !bufdesc.startIO(true)

	return bufdesc, found, nil
}

func (mgr *BufMgr) ReadBuffer(reln system.RelFileNode, block system.BlockNumber) (*Buffer, error) {
	bufdesc, found, err := mgr.bufferAlloc(reln, block)
}

type BufferStrategy interface {
	GetBuffer() (bufdesc *bufferDesc, isDirty bool)
}
