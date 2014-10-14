package system

import (
	"encoding/binary"
	"io"
	"strconv"
	"unsafe"
)

type Name string

const NameLen = 64

type Oid uint32

const InvalidOid Oid = 0

type Int4 int32

var BoolType Oid = 16
var ByteType Oid = 17
var CharType Oid = 18
var NameType Oid = 19
var Int8Type Oid = 20
var Int2Type Oid = 21
var Int4Type Oid = 23
var TextType Oid = 25
var OidType Oid = 26
var TidType Oid = 27
var XidType Oid = 28

type Datum interface {
	ToString() string
	FromString(str string) (Datum, error)
	ToBytes(writer io.Writer) (int, error)
	FromBytes(reader io.Reader) Datum
	Equals(other Datum) bool
	Len() int
}

type TypeInfo struct {
	Id   Oid
	Name Name
	Len  int16
	Zero Datum
}

var TypeRegistry = map[Oid]*TypeInfo{
	OidType: &TypeInfo{
		Id:   OidType,
		Name: Name("oid"),
		Len:  int16(unsafe.Sizeof(Oid(0))),
		Zero: Oid(0),
	},
	Int4Type: &TypeInfo{
		Id:   Int4Type,
		Name: Name("int4"),
		Len:  int16(unsafe.Sizeof(Int4(0))),
		Zero: Int4(0),
	},
	TidType: &TypeInfo{
		Id:   TidType,
		Name: Name("tid"),
		Len:  int16(unsafe.Sizeof(ItemPointer{0, 0})),
		Zero: ItemPointer{0, 0},
	},
	NameType: &TypeInfo{
		Id:   NameType,
		Name: Name("name"),
		Len:  NameLen,
		Zero: Name(""),
	},
}

const (
	TypeFixed   = iota
	TypeVarlena = iota
	TypeCString = iota
)

func (typ *TypeInfo) Category() int {
	if typ.Len == -2 {
		return TypeCString
	} else if typ.Len == -1 {
		return TypeVarlena
	} else {
		return TypeFixed
	}
}

func DatumFromString(str string, typid Oid) (Datum, error) {
	if entry, ok := TypeRegistry[typid]; ok {
		return entry.Zero.FromString(str)
	}

	panic("unknown type")
}

func DatumFromBytes(reader io.Reader, typid Oid) Datum {
	if entry, ok := TypeRegistry[typid]; ok {
		return entry.Zero.FromBytes(reader)
	}

	panic("unknown type")
}

func (val Name) ToString() string {
	return string(val)
}

func (val Name) FromString(str string) (Datum, error) {
	if len(str) > NameLen {
		return nil, Ereport(InvalidTextRepresentation, "value too long")
	}
	return Datum(Name(str)), nil
}

func (val Name) ToBytes(writer io.Writer) (int, error) {
	b := make([]byte, NameLen)
	copy(b, []byte(val))
	return writer.Write(b)
}

func (val Name) FromBytes(reader io.Reader) Datum {
	b := make([]byte, NameLen)
	n, err := reader.Read(b)
	if n != NameLen || err != nil {
		panic("read error")
	}
	for i := 0; i < NameLen; i++ {
		if b[i] == 0 {
			newval := string(b[0:i])
			return Datum(Name(newval))
		}
	}
	panic("read error")
}

func (val Name) Equals(other Datum) bool {
	if oval, ok := other.(Name); ok {
		return val == oval
	}
	return false
}

func (val Name) Len() int {
	return NameLen
}

func (val Oid) ToString() string {
	return strconv.Itoa(int(val))
}

func (val Oid) FromString(str string) (Datum, error) {
	num, err := strconv.ParseUint(str, 10, 0)
	if err != nil {
		err2 := err.(*strconv.NumError)
		return nil, Ereport(InvalidTextRepresentation, "%s", err2.Err)
	}
	return Datum(Oid(num)), nil
}

func (val Oid) ToBytes(writer io.Writer) (int, error) {
	err := binary.Write(writer, binary.LittleEndian, val)
	return val.Len(), err
}

func (val Oid) FromBytes(reader io.Reader) Datum {
	var newval Oid
	if err := binary.Read(reader, binary.LittleEndian, &newval); err != nil {
		panic("read error")
	}
	return Datum(newval)
}

func (val Oid) Equals(other Datum) bool {
	if oval, ok := other.(Oid); ok {
		return val == oval
	}
	return false
}

func (val Oid) Len() int {
	return 4
}

func (val Int4) ToString() string {
	return strconv.Itoa(int(val))
}

func (val Int4) FromString(str string) (Datum, error) {
	num, err := strconv.ParseInt(str, 10, 0)
	if err != nil {
		err2 := err.(*strconv.NumError)
		return nil, Ereport(InvalidTextRepresentation, "%s", err2.Err)
	}
	return Datum(Int4(num)), nil
}

func (val Int4) ToBytes(writer io.Writer) (int, error) {
	err := binary.Write(writer, binary.LittleEndian, val)
	return val.Len(), err
}

func (val Int4) FromBytes(reader io.Reader) Datum {
	var newval Int4
	if err := binary.Read(reader, binary.LittleEndian, &newval); err != nil {
		panic("read error")
	}
	return Datum(newval)
}

func (val Int4) Equals(other Datum) bool {
	if oval, ok := other.(Int4); ok {
		return val == oval
	}
	return false
}

func (val Int4) Len() int {
	return 4
}
