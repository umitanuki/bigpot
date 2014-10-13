package access

import (
	"unsafe"

	"bigpot/system"
)

type HeapTuple struct {
	tableOid system.Oid
	self     system.ItemPointer
	bytes    []byte
	data     *HeapTupleHeader
}

type HeapTupleFields struct {
	xmin system.Xid
	xmax system.Xid
	xvac system.Xid
}

type HeapTupleHeader struct {
	heap      HeapTupleFields
	ctid      system.ItemPointer
	infomask2 uint16
	infomask  uint16
	hoff      uint8
	bits      byte
}

const (
	heapHasNull        = 0x0001
	heapHasVarWidth    = 0x0002
	heapHasExternal    = 0x0004
	heapHasOid         = 0x0008
	heapXmaxKeyshrLock = 0x0010
	heapComboCid       = 0x0020
	heapXmaxExclLock   = 0x0040
	heapXmaxLockOnly   = 0x0080
	heapXmaxShrLock    = heapXmaxExclLock | heapXmaxKeyshrLock
	heapLockMask       = heapXmaxShrLock | heapXmaxExclLock | heapXmaxKeyshrLock
	heapXminCommitted  = 0x01000
	heapXminInvalid    = 0x0200
	heapXmaxCommitted  = 0x0400
	heapXmaxInvalid    = 0x0800
	heapXmaxIsMulti    = 0x1000
	heapUpdated        = 0x2000
	heapMovedOff       = 0x4000
	heapMovedIn        = 0x8000
	heapXactMask       = 0xfff0
)

func NewHeapTuple(bytes []byte, tid system.ItemPointer) *HeapTuple {
	tuple := &HeapTuple{
		self:  tid,
		bytes: bytes,
		data:  (*HeapTupleHeader)(unsafe.Pointer(&bytes[0])),
	}
	return tuple
}

func (tuple *HeapTuple) SetTableOid(oid system.Oid) {
	tuple.tableOid = oid
}

func (tuple *HeapTuple) SetData(bytes []byte, tid system.ItemPointer) {
	tuple.self = tid
	tuple.bytes = bytes
	tuple.data = (*HeapTupleHeader)(unsafe.Pointer(&bytes[0]))
}

func (htup *HeapTupleHeader) Xmin() system.Xid {
	return htup.heap.xmin
}

func (htup *HeapTupleHeader) SetXmin(xmin system.Xid) {
	htup.heap.xmin = xmin
}

func (htup *HeapTupleHeader) Xmax() system.Xid {
	return htup.heap.xmax
}

func (htup *HeapTupleHeader) SetXmax(xmax system.Xid) {
	htup.heap.xmax = xmax
}

func (htup *HeapTupleHeader) Oid() system.Oid {
	if htup.infomask&heapHasOid != 0 {
		ptr := uintptr(unsafe.Pointer(htup)) + uintptr(htup.hoff) - unsafe.Sizeof(system.Oid(0))
		return *(*system.Oid)(unsafe.Pointer(ptr))
	}
	return system.InvalidOid
}

func (tuple *HeapTuple) Get(attnum system.AttrNumber) system.Datum {
	if attnum <= 0 {
		switch attnum {
		case system.CtidAttrNumber:
			//return system.Datum(tuple.self)
		case system.OidAttrNumber:
			return system.Datum(tuple.data.Oid())
		case system.XminAttrNumber:
			//return system.Datum(tuple.data.Xmin())
		case system.CminAttrNumber:
			// TODO:
		case system.XmaxAttrNumber:
			//return system.Datum(tuple.data.Xmax())
		case system.CmaxAttrNumber:
			// TODO:
		case system.TableOidAttrNumber:
			return system.Datum(tuple.tableOid)
		}
	} else {

	}

	return nil
}

func FormHeapTuple(values []system.Datum, tupdesc TupleDesc) *HeapTuple {
	return nil
}
