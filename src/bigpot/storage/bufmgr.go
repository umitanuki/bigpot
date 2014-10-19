package storage

import (
	"fmt"
	"sync"

	"bigpot/system"
)

// This is the actual byte chunk of block size.
type Block [system.BlockSize]byte

type BufferManager interface {
	ReadBuffer(system.RelFileNode, system.BlockNumber) (Buffer, error)
	ReleaseBuffer(Buffer)
}

type Buffer interface {
	IsValid() bool
	// IsLocal() bool
	Lock()
	RLock()
	Unlock()
	GetPage() *Page
	MarkDirty()
}

type bufferTag struct {
	reln  system.RelFileNode
	block system.BlockNumber
}

type readBufferRes struct {
	bufDesc *bufferDesc
	err     error
}

type readBufferReq struct {
	tag bufferTag
	res chan readBufferRes
}

// Implements BufferManager
type bufMgr struct {
	lookup map[bufferTag]*bufferDesc
	// to be partitioned
	lookupLock  sync.RWMutex
	descriptors []bufferDesc
	pool        []Block
	readChan    chan readBufferReq
	releaseChan chan *bufferDesc
	smgr        Smgr
	nextVictim  int
}

// Implements Buffer
type bufferDesc struct {
	sync.RWMutex
	tag        bufferTag
	refCount   int
	usageCount int
	buffer     *Block
	isValid,
	isTagValid,
	isDirty bool
}

// A byte slice with BlockSize zeros.  Used to initialize blocks.
var _ZeroBlock = make([]byte, system.BlockSize)

const NewBlock = system.InvalidBlockNumber

// The maximum usage count for the clock sweep algorithm.  This is set
// a little higher than postgres, because we always bump up usageCount
// even if the same backend acquires the buffer.
const _MaxUsageCount = 10

// Allocates a new BufferManager, with the number of buffer nBuffers.
func NewBufferManager(nBuffers int) BufferManager {
	mgr := &bufMgr{
		lookup:      map[bufferTag]*bufferDesc{},
		descriptors: make([]bufferDesc, nBuffers),
		pool:        make([]Block, nBuffers),
		readChan:    make(chan readBufferReq),
		releaseChan: make(chan *bufferDesc),
		nextVictim:  0,
	}
	mgr.smgr = NewMdSmgr()
	// notice: range loop doesn't work because its a non-pointer slice.
	for i := 0; i < nBuffers; i++ {
		bufDesc := &mgr.descriptors[i]
		bufDesc.buffer = &mgr.pool[i]
	}
	go mgr.ioRoutine()

	return BufferManager(mgr)
}

// Implements BufferManager.ReadBuffer.  Upon return, the returned buffer
// is guaranteed to be pinned, until the caller releases the buffer.
func (mgr *bufMgr) ReadBuffer(reln system.RelFileNode, block system.BlockNumber) (Buffer, error) {
	resChan := make(chan readBufferRes)
	readParam := readBufferReq{
		bufferTag{reln, block}, resChan,
	}

	// Different from postgres, this implementation (for now) is serialized
	// so that the logic is simplified.  Will see how it is comparable to
	// the postgres' complex concurrent logic.
	// send a request
	mgr.readChan <- readParam

	// receive the result
	res := <-resChan
	return Buffer(res.bufDesc), res.err
}

func (mgr *bufMgr) ReleaseBuffer(buf Buffer) {
	bufDesc := buf.(*bufferDesc)

	// I don't think we need to make it synchronous.
	mgr.releaseChan <- bufDesc
}

// This is a background workhose goroutine that performs requested tasks.
func (mgr *bufMgr) ioRoutine() {
	for {
		select {
		case readParam := <-mgr.readChan:
			tag, res := readParam.tag, readParam.res
			bufDesc, err := mgr.readBufferInternal(tag)
			res <- readBufferRes{bufDesc, err}

		case bufDesc := <-mgr.releaseChan:
			bufDesc.unpin()
		}
	}
}

func (mgr *bufMgr) writeBuffer(buf *bufferDesc) error {
	smgr := mgr.smgr.GetRelation(buf.tag.reln)
	err := smgr.Write(buf.tag.block, buf.buffer)
	if err != nil {
		return err
	}
	buf.isDirty = false
	return err
}

// Returns a free buffer that is not used.
func (mgr *bufMgr) getUnusedBuffer() (*bufferDesc, error) {
	// We currently implement only clock sweep part
	// without free list, since clock sweep can return
	// free buffer anyway.  Free list might help some
	// performance gain, though.

	// run clock sweep
	nTry := len(mgr.pool)
	for nTry > 0 {
		buf := &mgr.descriptors[mgr.nextVictim]
		mgr.nextVictim++
		if mgr.nextVictim == len(mgr.pool) {
			mgr.nextVictim = 0
		}
		if buf.refCount == 0 {
			if buf.usageCount > 0 {
				// TODO: atomic
				buf.usageCount--
			} else {
				// Return buffer only if it's unpinned and least used.
				return buf, nil
			}
		} else {
			nTry--
		}
	}

	return nil, fmt.Errorf("no unpinned buffers available")
}

// Lookup the buffer table or re-useable list, and return it if found.
// The returned buffer is pinned and is already marked as holding the
// desired page.  If it already did have the desired page, "found" is
// set true.  Otherwise, "found" is set false, and the caller needs to do
// I/O to fill it.  "found" is redundant with buffer's isValid.
func (mgr *bufMgr) allocBuffer(tag bufferTag) (*bufferDesc, bool, error) {
	if buf, found := mgr.lookup[tag]; found {
		// Found it in the hash table.  Now, pin the buffer so no one can
		// steal it from buffer pool.
		buf.pin()

		return buf, found, nil
	}

	buf, err := mgr.getUnusedBuffer()
	if err != nil {
		return nil, false, err
	}
	buf.pin()

	if buf.isDirty {
		// If the buffer was dirty, try to write it out.
		if err := mgr.writeBuffer(buf); err != nil {
			return nil, false, err
		}
	}

	// update hash table: remove old entry.
	// delete() don't care if tag does not exist
	delete(mgr.lookup, buf.tag)

	// it's all ours
	mgr.lookup[tag] = buf
	buf.tag = tag
	buf.isValid = false
	buf.isDirty = false
	buf.isTagValid = true

	// reset usage count, as we renamed the buffer.  (The usageCount starts
	// out at 1 so that the buffer can survive one clock-sweep pass.)
	buf.usageCount = 1

	return buf, false, nil
}

// The main task of ReadBuffer.  This is simplified under the assumption
// that no concurrent access to the shared extent is happening, and made
// much similar to what postgres has as local buffers.
func (mgr *bufMgr) readBufferInternal(tag bufferTag) (*bufferDesc, error) {
	blockNum := tag.block

	smgr := mgr.smgr.GetRelation(tag.reln)

	isExtend := blockNum == NewBlock

	// Substitute proper block number if called asked
	if isExtend {
		bnum, err := smgr.NBlocks()
		if err != nil {
			return nil, err
		}
		tag.block = bnum
		blockNum = bnum
	}

	// lookup the buffer.
	buf, found, err := mgr.allocBuffer(tag)
	if err != nil {
		return nil, err
	}

	// if it was already in the buffer pool, we're done
	if found {
		if !isExtend {
			return buf, nil
		}
		// TODO: not sure if this occurs...

		buf.isValid = false
	}

	// We have allocated a buffer for the page but its contents are
	// not yet valid.
	if isExtend {
		// new buffers are zero-filled
		copy(buf.buffer[:], _ZeroBlock)
		if err := smgr.Extend(blockNum, buf.buffer); err != nil {
			buf.unpin()
			buf.isValid = false
			delete(mgr.lookup, tag)
			return nil, err
		}
	} else {
		// Read in the page.  We may want to make this async I/O later.
		if err := smgr.Read(blockNum, buf.buffer); err != nil {
			buf.unpin()
			buf.isValid = false
			delete(mgr.lookup, tag)
			return nil, err
		}
	}

	buf.isValid = true

	return buf, nil
}

var invalidBuffer *bufferDesc = nil

func InvalidBuffer() Buffer {
	return invalidBuffer
}

func (buf *bufferDesc) IsValid() bool {
	return buf != invalidBuffer
}

func (buf *bufferDesc) GetPage() *Page {
	return NewPage(buf.buffer)
}

func (buf *bufferDesc) MarkDirty() {
	buf.isDirty = true
}

func (buf *bufferDesc) pin() {
	buf.refCount++

	if buf.usageCount < _MaxUsageCount {
		buf.usageCount++
	}
}

func (buf *bufferDesc) unpin() {
	buf.refCount--
}
