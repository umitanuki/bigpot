package storage

import (
	"encoding/binary"
	"fmt"
	"log"
	"unsafe"

	"bigpot/system"
)

type pageHeader struct {
	lsn                   system.Lsn
	checksum, flags       uint16
	lower, upper, special uint16 // TODO: LocationIndex (15bit)
	pagesize_version      uint16
	prune_xid             system.Xid
	linp                  ItemId
}

// line pointer(s) do not count as part of header
const sizeOfPageHeader = uint16(unsafe.Offsetof(pageHeader{}.linp))
const LayoutVersion = uint16(4)

// _PageHeader.flags contains the following flag bits.  Undefined bits are initialized
// to zero and may be used in the future.
//
// _PageHasFreeLines is set if there are any itemIdUnused line pointers before
// lower.  This should be considered a hint rather than the truth, since
// changes to it are not WAL-logged.
//
// _PageFull is set if an UPDATE doesn't find enough free space in the
// page for its new tuple version; this suggests that a prune is needed.
// Again, this is just a hint.
const (
	_PageHasFreeLines  = uint16(0x0001)
	_PageFull          = uint16(0x0002)
	_PageAllVisible    = uint16(0x0004)
	_PageValidFlagBits = uint16(0x0007)
)

var _PageZero = make([]byte, system.BlockSize)

// A postgres disk page is an abstraction layered on top of a postgres
// disk block (which is simply a unit of i/o, see block.h).
//
// specifically, while a disk block can be unformatted, a postgres
// disk page is always a slotted page of the form:
//
// +----------------+---------------------------------+
// | PageHeaderData | linp1 linp2 linp3 ...           |
// +-----------+----+---------------------------------+
// | ... linpN |                                      |
// +-----------+--------------------------------------+
// |           ^ pd_lower                             |
// |                                                  |
// |             v pd_upper                           |
// +-------------+------------------------------------+
// |             | tupleN ...                         |
// +-------------+------------------+-----------------+
// |       ... tuple3 tuple2 tuple1 | "special space" |
// +--------------------------------+-----------------+
//                                  ^ pd_special
//
// a page is full when nothing can be added between pd_lower and
// pd_upper.
//
// all blocks written out by an access method must be disk pages.
//
// EXCEPTIONS:
//
// obviously, a page is not formatted before it is initialized by
// a call to PageInit.
//
// NOTES:
//
// linp1..N form an ItemId array.  ItemPointers point into this array
// rather than pointing directly to a tuple.  Note that OffsetNumbers
// conventionally start at 1, not 0.
//
// tuple1..N are added "backwards" on the page.  because a tuple's
// ItemPointer points to its ItemId entry rather than its actual
// byte-offset position, tuples can be physically shuffled on a page
// whenever the need arises.
//
// AM-generic per-page information is kept in PageHeaderData.
//
// AM-specific per-page data (if any) is kept in the area marked "special
// space"; each AM has an "opaque" structure defined somewhere that is
// stored as the page trailer.	an access method should always
// initialize its pages with PageInit and then set its own opaque
// fields.
type Page struct {
	header *pageHeader
	bytes  *Block
}

func NewPage(b *Block) *Page {
	if len(b) != system.BlockSize {
		panic("invalid block bytes")
	}
	header := (*pageHeader)(unsafe.Pointer(&b[0]))
	return &Page{
		header: header,
		bytes:  b,
	}
}

func (page *Page) Lsn() system.Lsn {
	return page.header.lsn
}

func (page *Page) SetLsn(lsn system.Lsn) {
	page.header.lsn = lsn
}

func (page *Page) Flags() uint16 {
	return page.header.flags
}

func (page *Page) SetFlags(flags uint16) {
	page.header.flags = flags
}

func (page *Page) HasFreeLinePointers() bool {
	return (page.header.flags & _PageHasFreeLines) != 0
}

func (page *Page) SetHasFreeLinePointers() {
	page.header.flags |= _PageHasFreeLines
}

func (page *Page) ClearHasFreeLinePointers() {
	page.header.flags &= ^_PageHasFreeLines
}

func (page *Page) IsFull() bool {
	return (page.header.flags & _PageFull) != 0
}

func (page *Page) SetFull() {
	page.header.flags |= _PageFull
}

func (page *Page) ClearFull() {
	page.header.flags &= ^_PageFull
}

func (page *Page) IsAllVisible() bool {
	return (page.header.flags & _PageAllVisible) != 0
}

func (page *Page) SetAllVisible() {
	page.header.flags |= _PageAllVisible
}

func (page *Page) ClearAllVisible() {
	page.header.flags &= ^_PageAllVisible
}

func (page *Page) IsPrunable(oldestxmin system.Xid) bool {
	if !oldestxmin.IsNormal() {
		panic("invalid argument")
	}
	if page.header.prune_xid.IsValid() {
		return page.header.prune_xid.Precedes(oldestxmin)
	}
	return false
}

func (page *Page) ClearPrunable() {
	page.header.prune_xid = system.InvalidXid
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

func (page *Page) PageSize() uint16 {
	return page.header.pagesize_version & uint16(0xff00)
}

func (page *Page) PageLayourVersion() uint16 {
	return page.header.pagesize_version & uint16(0x00ff)
}

func (page *Page) SetPageSizeAndVersion(size, version uint16) {
	if size&uint16(0xff00) != size {
		panic("invalid size")
	}
	if version&uint16(0x00ff) != version {
		panic("invalid version")
	}
	page.header.pagesize_version = size | version
}

func (page *Page) IsNew() bool {
	return page.Upper() == 0
}

func (page *Page) IsEmpty() bool {
	return page.Lower() <= sizeOfPageHeader
}

func (page *Page) IsValid() bool {
	return page.bytes != nil
}

// Returns an item identifier of a page.
func (page *Page) ItemId(offset system.OffsetNumber) *ItemId {
	addr := sizeOfPageHeader + uint16(offset-1)*uint16(unsafe.Sizeof(ItemId(0)))
	return (*ItemId)(unsafe.Pointer(&page.bytes[addr]))
}

// Returns the maximum offset number used by the given page.
// Since offset numbers are 1-based, this is also the number
// of items on the page.  If the page is not initialized (pd_lower == 0),
// we must return zero to ensure sane behavior.
func (page *Page) MaxOffsetNumber() system.OffsetNumber {
	lower := int16(page.Lower())
	offbytes := lower - int16(sizeOfPageHeader)
	if offbytes <= 0 {
		return 0
	} else {
		itemid_size := unsafe.Sizeof(ItemId(0))
		return system.OffsetNumber(uint16(offbytes) / uint16(itemid_size))
	}
}

// Initializes the content of a page.
func (page *Page) Init(specialSize uintptr) {
	copy(page.bytes[:], _PageZero)

	specialSize = system.MaxAlign(specialSize)
	offsetSpecial := uint16(system.BlockSize - specialSize)
	page.SetLower(sizeOfPageHeader)
	page.SetUpper(offsetSpecial)
	page.SetSpecial(offsetSpecial)
	page.SetPageSizeAndVersion(system.BlockSize, LayoutVersion)
}

func (page *Page) AddItem(item []byte, offset system.OffsetNumber, overwrite, is_heap bool) system.OffsetNumber {
	// Be wary about corrupted page pointers
	if page.Lower() < sizeOfPageHeader ||
		page.Lower() > page.Upper() ||
		page.Upper() > page.Special() ||
		page.Special() > system.BlockSize {
		log.Panicf("corrupted page pointers: lower = %d, upper = %d, special = %d",
			page.Lower(), page.Upper(), page.Special())
	}

	// Select offset to place the new item at
	limit := page.MaxOffsetNumber().Next()

	needshuffle := false

	// was offset passed in?
	if offset.IsValid() {
		// yes, check it
		if overwrite {
			if offset < limit {
				itemId := page.ItemId(offset)
				if itemId.IsUsed() || itemId.HasStorage() {
					log.Println("WARNING: will not overwrite a used ItemId")
					return system.InvalidOffsetNumber
				}
			}
		} else {
			if offset < limit {
				// need to move existing linp's
				needshuffle = true
			}
		}
	} else {
		if page.HasFreeLinePointers() {
			for offset = 1; offset < limit; offset = offset.Next() {
				itemId := page.ItemId(offset)
				if !itemId.IsUsed() && !itemId.HasStorage() {
					break
				}
			}
			if offset >= limit {
				page.ClearHasFreeLinePointers()
			}
		} else {
			// don't bother searching if hint says there's no free slot
			offset = limit
		}
	}

	if offset > limit {
		log.Println("WARNING: specified item offset is too large")
		return system.InvalidOffsetNumber
	}

	// if is_heap && offset > MaxHeapTuplesPerPage {
	// 	log.Println("WARNING: can't put more than MaxHeapTuplesPerPage items in a heap page")
	// 	return InvalidOffsetNumber
	// }

	// Compute new lower and upper pointers for page, see if it'll fit.
	// Note: do arithmetic as signed ints, to avoid mistakes if, say,
	// alignedSize > upper.
	lower := int(page.Lower())
	if offset == limit || needshuffle {
		lower = lower + int(unsafe.Sizeof(ItemId(0)))
	}

	alignedSize := system.MaxAlign(uintptr(len(item)))

	upper := int(page.Upper()) - int(alignedSize)

	if lower > upper {
		return system.InvalidOffsetNumber
	}

	// OK to insert the item.  First, shuffle the existing pointers if needed.
	itemId := page.ItemId(offset)

	if needshuffle {
		destItemId := page.ItemId(offset + 1)
		base := uintptr(unsafe.Pointer(&page.bytes[0]))
		destStart := uintptr(unsafe.Pointer(destItemId)) - base
		srcStart := uintptr(unsafe.Pointer(itemId)) - base
		copyLen := uintptr(limit-offset) * unsafe.Sizeof(ItemId(0))
		dest := page.bytes[destStart : destStart+copyLen]
		src := page.bytes[srcStart : srcStart+copyLen]
		copy(dest, src)
	}

	// set the item pointer
	itemId.SetNormal(uint(upper), uint(len(item)))

	// copy the item's data onto the page
	if l := copy(page.bytes[upper:], item); l != len(item) {
		panic("unexpected copy result")
	}

	page.SetLower(uint16(lower))
	page.SetUpper(uint16(upper))

	return offset
}

// Retrieves an item on the given page.
// Note: This does not change the status of any of the resources passed.
// The semantics may change in the future.
func (page *Page) Item(itemId *ItemId) []byte {
	offset := itemId.Offset()
	length := itemId.Length()
	return page.bytes[offset : offset+length]
}

// Returns the size of the free (allocatable) space on a page,
// reduced by the space needed for a new line pointer.
// Note: this should usually only be used on index pages.  Use
// Page.HeapFreeSpace on heap pages.
func (page *Page) FreeSpace() uint {
	space := int(page.Upper()) - int(page.Lower())

	if space < int(unsafe.Sizeof(ItemId(0))) {
		return 0
	}
	space -= int(unsafe.Sizeof(ItemId(0)))

	return uint(space)
}

// An item pointer (also called line pointer) on a buffer page
//
// In some cases an item pointer is "in use" but does not have any associated
// storage on page.  By convention, length == 0 in every item pointer
// that does not have storage, independently of its flags state.
type ItemId uint32

const (
	itemIdUnused   = uint(0) // unused (should always have length == 0
	itemIdNormal   = uint(1) // used (should always have length > 0
	itemIdRedirect = uint(2) // HOT redirect (should have length == 0)j
	itemIdDead     = uint(3) // dead, may or may not have storage
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
	val := ItemId((flags & 0x3) << 15)
	*itid = (val | (*itid & 0xFFFE7FFF))
}

func (itid *ItemId) Length() uint {
	return uint(*itid & 0x00007FFF)
}

func (itid *ItemId) SetLength(length uint) {
	val := ItemId(length & 0x7FFF)
	*itid = (val | (*itid & 0xFFFF8000))
}

// True iff item identifier is in use.
func (itid *ItemId) IsUsed() bool {
	return itid.Flags() != itemIdUnused
}

// True iff item identifier is in state NORMAL.
func (itid *ItemId) IsNormal() bool {
	return itid.Flags() == itemIdNormal
}

// True iff item identifier is in state REDIRECT.
func (itid *ItemId) IsRedirected() bool {
	return itid.Flags() == itemIdRedirect
}

// True iff item identifier is in state DEAD.
func (itid *ItemId) IsDead() bool {
	return itid.Flags() == itemIdDead
}

// True iff item identifier has associated storage.
func (itid *ItemId) HasStorage() bool {
	return itid.Length() != 0
}

// Set the item identifier to be UNUSED, with no storage.
func (itid *ItemId) SetUnused() {
	itid.SetFlags(itemIdUnused)
	itid.SetOffset(0)
	itid.SetLength(0)
}

// Set the item identifier to be NORMAL, with the specified storage.
func (itid *ItemId) SetNormal(offset, length uint) {
	itid.SetFlags(itemIdNormal)
	itid.SetOffset(offset)
	itid.SetLength(length)
}

// Set the item identifier to be REDIRECT, with the specified link.
func (itid *ItemId) SetRedirect(link uint) {
	itid.SetFlags(itemIdRedirect)
	itid.SetOffset(link)
	itid.SetLength(0)
}

// Set the item identifier to be DEAD, with no storage.
func (itid *ItemId) SetDead() {
	itid.SetFlags(itemIdDead)
	itid.SetOffset(0)
	itid.SetLength(0)
}

// Set the item identifier to be DEAD, keeping its existing storage.
//
// Note: in indexes, this is used as if it were a hint-bit mechanism;
// we trust that multiple processors can do this in parallel and get
// the same result.
func (itid *ItemId) MarkDead() {
	itid.SetFlags(itemIdDead)
}

func (itid *ItemId) String() string {
	return fmt.Sprintf("ItemId(flags=%d, offset=%d, length=%d)", itid.Flags(), itid.Offset(), itid.Length())
}
