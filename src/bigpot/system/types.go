package system

import "strconv"

type Name string
type Oid uint32
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

type Datum interface {
	ToString() string
	Equals(other Datum) bool
}

func DatumFromString(str string, typid Oid) Datum {
	switch typid {
	case OidType:
		num, _ := strconv.Atoi(str)
		return Datum(Oid(num))
	case NameType:
		return Datum(Name(str))
	case Int4Type:
		num, _ := strconv.Atoi(str)
		return Datum(Int4(num))
	}
	return nil
}

func (val Name) ToString() string {
	return string(val)
}

func (val Name) Equals(other Datum) bool {
	if oval, ok := other.(Name); ok {
		return val == oval
	}
	return false
}

func (val Oid) ToString() string {
	return strconv.Itoa(int(val))
}

func (val Oid) Equals(other Datum) bool {
	if oval, ok := other.(Oid); ok {
		return val == oval
	}
	return false
}

func (val Int4) ToString() string {
	return strconv.Itoa(int(val))
}

func (val Int4) Equals(other Datum) bool {
	if oval, ok := other.(Int4); ok {
		return val == oval
	}
	return false
}
