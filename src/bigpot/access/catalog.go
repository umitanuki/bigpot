package access

import (
	"bigpot/system"
)

var ClassRelId system.Oid = 1259
var ClassTupleDesc = &TupleDesc{
	Attrs: []*Attribute{
		{
			Name:   "relname",
			TypeId: system.NameType,
		},
		{
			Name:   "relfilenode",
			TypeId: system.OidType,
		},
	},
	typid:  ClassRelId,
	hasOid: true,
}

const (
	Anum_class_relname     = 1
	Anum_clasS_relfilenode = 2
)

var AttributeRelId system.Oid = 1249
var AttributeTupleDesc = &TupleDesc{
	Attrs: []*Attribute{
		{
			Name:   "attrelid",
			TypeId: system.OidType,
		},
		{
			Name:   "attname",
			TypeId: system.NameType,
		},
		{
			Name:   "attnum",
			TypeId: system.Int4Type,
		},
		{
			Name:   "atttypid",
			TypeId: system.OidType,
		},
	},
	typid:  AttributeRelId,
	hasOid: false,
}

const (
	Anum_attribute_attrelid = 1
	Anum_attribute_attname  = 2
	Anum_attribute_attnum   = 3
	Anum_attribute_atttypid = 4
)

func initTupleDesc(tupdesc *TupleDesc) {
	for _, attr := range tupdesc.Attrs {
		attr.Type = system.TypeRegistry[attr.TypeId]
	}
}

func init() {
	initTupleDesc(ClassTupleDesc)
	initTupleDesc(AttributeTupleDesc)
}
