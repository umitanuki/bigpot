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

var _ZeroBlock = make([]byte, system.BlockSize)

const NewBlock = system.InvalidBlockNumber
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

func (mgr *bufMgr) ReadBuffer(reln system.RelFileNode, block system.BlockNumber) (Buffer, error) {
	resChan := make(chan readBufferRes)
	readParam := readBufferReq{
		bufferTag{reln, block}, resChan,
	}

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
	return smgr.Write(buf.tag.block, buf.buffer)
}

func (mgr *bufMgr) getUnusedBuffer() (*bufferDesc, error) {
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
				return buf, nil
			}
		} else {
			nTry--
		}
	}

	return nil, fmt.Errorf("no unpinned buffers available")
}

func (mgr *bufMgr) allocBuffer(tag bufferTag) (*bufferDesc, bool, error) {
	if buf, found := mgr.lookup[tag]; found {
		buf.pin()

		return buf, found, nil
	}

	buf, err := mgr.getUnusedBuffer()
	if err != nil {
		return nil, false, err
	}
	buf.pin()

	if buf.isDirty {
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

	// reset usage count, as we renamed the buffer.
	buf.usageCount = 1

	return buf, false, nil
}

func (mgr *bufMgr) readBufferInternal(tag bufferTag) (*bufferDesc, error) {
	blockNum := tag.block

	smgr := mgr.smgr.GetRelation(tag.reln)

	isExtend := blockNum == NewBlock
	if isExtend {
		blockNum, err := smgr.NBlocks()
		if err != nil {
			return nil, err
		}
		tag.block = blockNum
	}
	buf, found, err := mgr.allocBuffer(tag)
	if err != nil {
		return nil, err
	}

	if found {
		if !isExtend {
			return buf, nil
		}

		buf.isValid = false
	}

	if isExtend {
		copy(buf.buffer[:], _ZeroBlock)
		smgr.Extend(blockNum, buf.buffer)
	} else {
		smgr.Read(blockNum, buf.buffer)
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

func (buf *bufferDesc) pin() {
	buf.refCount++

	if buf.usageCount < _MaxUsageCount {
		buf.usageCount++
	}
}

func (buf *bufferDesc) unpin() {
	buf.refCount--
}
