package storage

import (
	"encoding/binary"
	"fmt"
	"log"
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

// line pointer(s) do not count as part of header
const sizeOfPageHeader = uint16(unsafe.Offsetof(pagelayour.linp))
const LayoutVersion = uint16(4)

/*
 * pageHeader.flags contains the following flag bits.  Undefined bits are initialized
 * to zero and may be used in the future.
 *
 * pageHasFreeLines is set if there are any LP_UNUSED line pointers before
 * lower.  This should be considered a hint rather than the truth, since
 * changes to it are not WAL-logged.
 *
 * pageFull is set if an UPDATE doesn't find enough free space in the
 * page for its new tuple version; this suggests that a prune is needed.
 * Again, this is just a hint.
 */
const (
	pageHasFreeLines = uint16(0x0001)
	pageFull = uint16(0x0002)
	pageAllVisible = uint16(0x0004)
	pageValidFlagBits = uint16(0x0007)
)

var pageZero = make([]byte, system.BlockSize)

type Page struct {
	header		*pageHeader
	bytes		[]byte
}

func NewPage(b []byte) *Page {
	if len(b) != system.BlockSize {
		panic("invalid block bytes")
	}
	header := (*pageHeader)(unsafe.Pointer(&b[0]))
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

func (page *Page) Flags() uint16 {
	return page.header.flags
}

func (page *Page) SetFlags(flags uint16) {
	page.header.flags = flags
}

func (page *Page) HasFreeLinePointers() bool {
	return (page.header.flags & pageHasFreeLines) != 0
}

func (page *Page) SetHasFreeLinePointers() {
	page.header.flags |= pageHasFreeLines
}

func (page *Page) ClearHasFreeLinePointers() {
	page.header.flags &= ^pageHasFreeLines
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
	if size & uint16(0xff00) != size {
		panic("invalid size")
	}
	if version & uint16(0x00ff) != version {
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
	addr := sizeOfPageHeader + uint16(offset-1) * uint16(unsafe.Sizeof(ItemId(0)))
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
	copy(page.bytes, pageZero)

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
		copyLen := uintptr(limit - offset) * unsafe.Sizeof(ItemId(0))
		dest := page.bytes[destStart:destStart+copyLen]
		src := page.bytes[srcStart:srcStart+copyLen]
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
	return page.bytes[offset:offset+length]
}

// An item pointer (also called line pointer) on a buffer page
//
// In some cases an item pointer is "in use" but does not have any associated
// storage on page.  By convention, length == 0 in every item pointer
// that does not have storage, independently of its flags state.
type ItemId uint32

const (
	ItemIdUnused = uint(0)		// unused (should always have length == 0
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
	return itid.Flags() != ItemIdUnused
}

// True iff item identifier has associated storage.
func (itid *ItemId) HasStorage() bool {
	return itid.Length() != 0
}

func (itid *ItemId) SetNormal(offset, length uint) {
	itid.SetFlags(ItemIdNormal)
	itid.SetOffset(offset)
	itid.SetLength(length)
}

func (itid *ItemId) String() string {
	return fmt.Sprintf("ItemId(flags=%d, offset=%d, length=%d)", itid.Flags(), itid.Offset(), itid.Length())
}
