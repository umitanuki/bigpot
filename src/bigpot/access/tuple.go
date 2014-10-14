package access

import (
	"bytes"
	"errors"
	"unsafe"

	"bigpot/system"
)

type HeapTuple struct {
	tableOid system.Oid
	self     system.ItemPointer
	tupdesc  *TupleDesc
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
	// information stored in infomask
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
	heapXminCommitted  = 0x0100
	heapXminInvalid    = 0x0200
	heapXmaxCommitted  = 0x0400
	heapXmaxInvalid    = 0x0800
	heapXmaxIsMulti    = 0x1000
	heapUpdated        = 0x2000
	heapMovedOff       = 0x4000
	heapMovedIn        = 0x8000
	heapXactMask       = 0xfff0

	// information stored in infomask2
	heapNattsMask = 0x07ff
)

func NewHeapTuple(bytes []byte, tupdesc *TupleDesc, tid system.ItemPointer) *HeapTuple {
	tuple := &HeapTuple{
		self:    tid,
		tupdesc: tupdesc,
		bytes:   bytes,
		data:    (*HeapTupleHeader)(unsafe.Pointer(&bytes[0])),
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

func (htup *HeapTupleHeader) HasNulls() bool {
	return htup.infomask&heapHasNull != 0
}

func (htup *HeapTupleHeader) IsNull(attnum system.AttrNumber) bool {
	if htup.HasNulls() {
		// TODO: maybe bytes should be with HeapTupleHeader.
		ptr := uintptr(unsafe.Pointer(&htup.bits))
		ptr += uintptr(((attnum) - 1) >> 3)
		bit := *(*byte)(unsafe.Pointer(ptr))
		// TODO: need bitmap accessor
		return (bit & byte(1<<(uint(attnum-1)&0x07))) == 0
	}
	return false
}

func (htup *HeapTupleHeader) Natts() system.AttrNumber {
	return system.AttrNumber(htup.infomask2 & uint16(heapNattsMask))
}

func (htup *HeapTupleHeader) SetNatts(attnum system.AttrNumber) {
	htup.infomask2 = (htup.infomask2 & ^uint16(heapNattsMask)) | uint16(attnum)
}

func (tuple *HeapTuple) Fetch(attnum system.AttrNumber) system.Datum {
	if attnum <= 0 {
		switch attnum {
		case system.CtidAttrNumber:
			return system.Datum(tuple.self)
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
		td := tuple.data

		if td.IsNull(attnum) {
			return nil
		}

		// TODO: attcache
		offset := int(td.hoff)
		for i := system.AttrNumber(1); i < attnum; i++ {
			if td.IsNull(i) {
				continue
			}
			attr := tuple.tupdesc.Attrs[i-1]
			if attr.Type.IsVarlen() {
				// TODO:
			} else {
				offset += int(attr.Type.Len)
			}
		}

		reader := bytes.NewReader(tuple.bytes[offset:])
		return system.DatumFromBytes(reader, tuple.tupdesc.Attrs[attnum-1].TypeId)
	}

	return nil
}

func bitmapLength(n int) int {
	return (n + 7) / 8
}

func computeHeapDataSize(values []system.Datum, tupdesc *TupleDesc) uintptr {
	var data_length uintptr = 0

	for i := 0; i < len(tupdesc.Attrs); i++ {
		val := values[i]

		if val == nil {
			continue
		}

		// TODO: varlena
		data_length += uintptr(val.Len())
	}

	return data_length
}

// bytesWriter implements Writer interface over existing []byte.
// This is different from bytes.Buffer in that it writes into the given buffer.
type bytesWriter []byte

// Implements Writer.Write().
func (writer *bytesWriter) Write(b []byte) (int, error) {
	n := copy(*writer, b)
	if n < len(b) {
		return n, errors.New("not enough bytes")
	}
	*writer = (*writer)[n:]
	return n, nil
}

func (htup *HeapTupleHeader) fill(values []system.Datum, tupdesc *TupleDesc, bits, data []byte) {
	htup.infomask &= ^uint16(heapHasNull | heapHasVarWidth | heapHasExternal)

	bitIndex := -1
	highBit := byte(0x80)
	bitmask := highBit
	writer := (*bytesWriter)(&data)
	for i := 0; i < len(tupdesc.Attrs); i++ {
		if bits != nil {
			if bitmask != highBit {
				bitmask <<= 1
			} else {
				bitIndex++
				bits[bitIndex] = 0
				bitmask = 1
			}

			if values[i] == nil {
				htup.infomask |= uint16(heapHasNull)
				continue
			}
			bits[bitIndex] |= bitmask
		}

		// no alignment for now
		// TODO: set infomask with varsize
		values[i].ToBytes(writer)
	}
}

func FormHeapTuple(values []system.Datum, tupdesc *TupleDesc) *HeapTuple {
	natts := len(tupdesc.Attrs)
	hasnull := false
	for _, value := range values {
		if value == nil {
			hasnull = true
			//} else if att.attlen == -1
			// TODO: flatten toast value
		}
	}

	length := unsafe.Offsetof(HeapTupleHeader{}.bits)

	if hasnull {
		length += uintptr(bitmapLength(natts))
	}
	if tupdesc.hasOid {
		length += unsafe.Sizeof(system.Oid(0))
	}

	length = system.MaxAlign(length)
	hoff := uint8(length)

	data_len := computeHeapDataSize(values, tupdesc)
	length += data_len

	tuple_data := make([]byte, length)
	tuple := &HeapTuple{
		tupdesc:  tupdesc,
		tableOid: system.InvalidOid,
	}
	tuple.SetData(tuple_data, system.InvalidItemPointer)

	td := tuple.data
	td.SetNatts(system.AttrNumber(natts))
	td.hoff = hoff

	if tupdesc.hasOid {
		td.infomask = heapHasOid
	}

	bits := []byte(nil)
	if hasnull {
		bits = tuple.bytes[unsafe.Offsetof(td.bits):hoff]
	}
	data := tuple.bytes[hoff:]
	td.fill(values, tupdesc, bits, data)

	return tuple
}
