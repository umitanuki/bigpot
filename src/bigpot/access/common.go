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
	Get(attnum system.AttrNumber) system.Datum
}

type Attribute struct {
	AttName system.Name
	AttType system.Oid
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
