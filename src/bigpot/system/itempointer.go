package system

import (
	"encoding/binary"
	"fmt"
	"io"
)

const BlockSize = 4 * 1024

// BlockNumber
type BlockNumber uint32

const InvalidBlockNumber = 0xFFFFFFFF
const MaxBlockNumber = 0xFFFFFFFE

func (block BlockNumber) IsValid() bool {
	return block != InvalidBlockNumber
}

// OffsetNumber
type OffsetNumber uint16

const InvalidOffsetNumber = 0
const FirstOffsetNumber = 1
const MaxOffsetNumber = BlockSize / 2

func (offset OffsetNumber) IsValid() bool {
	return offset != InvalidOffsetNumber && offset <= MaxOffsetNumber
}

// ItemPointer
type ItemPointer struct {
	block  BlockNumber
	offset OffsetNumber
}

func (itemptr *ItemPointer) ToString() string {
	return fmt.Sprintf("(%u,%u)", itemptr.block, itemptr.offset)
}

func (itemptr *ItemPointer) FromBytes(reader io.Reader) Datum {
	var newval ItemPointer
	if err := binary.Read(reader, binary.LittleEndian, &newval.block); err != nil {
		panic("read error")
	}
	if err := binary.Read(reader, binary.LittleEndian, &newval.offset); err != nil {
		panic("read error")
	}
	return Datum(&newval)
}

func (itemptr *ItemPointer) Equals(other Datum) bool {
	if oval, ok := other.(*ItemPointer); ok {
		return itemptr.block == oval.block && itemptr.offset == oval.offset
	}
	return false
}

func (itemptr *ItemPointer) Len() int {
	// sizeof(BlockNumber) + sizeof(OffsetNumber)
	return 6
}
