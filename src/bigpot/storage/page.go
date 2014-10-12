package storage

import (
	"encoding/binary"
	"unsafe"

	"bigpot/system"
)

type pageHeader struct {
	lsn system.Lsn
	checksum, flags uint16
	lower, upper, special uint16 // TODO: LocationIndex (15bit)
	pagesize_version uint16
	prune_xid system.Xid
	linp ItemId
}

var pagelayour pageHeader
const pageHeaderSize = unsafe.Offsetof(pagelayour.linp)

type Page struct {
	header		*pageHeader
	bytes		[]byte
}

func NewPage(b []byte) *Page {
	if len(b) != system.BlockSize {
		panic("invalid block bytes")
	}
	header := *(**pageHeader)(unsafe.Pointer(&b))
	return &Page{
		header: header,
		bytes: b,
	}
}

func (page *Page) Lsn() system.Lsn {
	return page.header.lsn
}

func (page *Page) SetLsn(lsn system.Lsn) {
	page.header.lsn = lsn
}

func (page *Page) Lower() uint16 {
	return page.header.lower
}

func (page *Page) SetLower(lower uint16) {
	page.header.lower = lower
}

func (page *Page) Upper() uint16 {
	return page.header.upper
}

func (page *Page) SetUpper(upper uint16) {
	page.header.upper = upper
}

func (page *Page) Special() uint16 {
	return page.header.special
}

func (page *Page) SetSpecial(special uint16) {
	page.header.special = special
}

func (page *Page) IsNew() bool {
	return page.Upper() == 0
}

func (page *Page) IsEmpty() bool {
	return uintptr(page.Lower()) <= pageHeaderSize
}

func (page *Page) IsValid() bool {
	return page.bytes != nil
}

func (page *Page) Item(offset system.OffsetNumber) *ItemId {
	addr := pageHeaderSize + uintptr(offset) * 4
	return NewItemId(page.bytes[addr:])
}

// An item pointer (also called line pointer) on a buffer page
//
// In some cases an item pointer is "in use" but does not have any associated
// storage on page.  By convention, length == 0 in every item pointer
// that does not have storage, independently of its flags state.
type ItemId uint32

const (
	ItemIdUsed = uint(0)		// unused (should always have length == 0
	ItemIdNormal = uint(1)		// used (should always have length > 0
	ItemIdRedirect = uint(2)	// HOT redirect (should have length == 0)j
	ItemIdDead = uint(3)		// dead, may or may not have storage
)

func NewItemId(b []byte) *ItemId {
	val := ItemId(binary.LittleEndian.Uint32(b))
	return &val
}

func (itid *ItemId) Offset() uint {
	return uint((*itid & 0xFFFE0000) >> 17)
}

func (itid *ItemId) SetOffset(offset uint) {
	val := ItemId((offset & 0x7FFF) << 17)
	*itid = (val | (*itid & 0x0001FFFF))
}

func (itid *ItemId) Flags() uint {
	return uint((*itid & 0x00018000) >> 15)
}

func (itid *ItemId) SetFlags(flags uint) {
	val := ItemId((flags & 0x2) << 15)
	*itid = (val | (*itid & 0xFFFE7FFF))
}

func (itid *ItemId) Length() uint {
	return uint(*itid & 0x00007FFF)
}

func (itid *ItemId) SetLength(length uint) {
	val := ItemId(length & 0x7FFF)
	*itid = (val | (*itid & 0xFFFF8000))
}
