package access

import (
	"bigpot/system"
)

type Relation interface {
	Close() error
}

type Scan interface {
	Next() (Tuple, error)
	EndScan() error
}

type Tuple interface {
	Fetch(attnum system.AttrNumber) system.Datum
}

type Attribute struct {
	Name   system.Name
	TypeId system.Oid
	Type   *system.TypeInfo
}

type TupleDesc struct {
	Attrs  []*Attribute
	typid  system.Oid
	hasOid bool
}

type ScanKey struct {
	AttNum int32
	Val    system.Datum
}
