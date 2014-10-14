package access

import (
	"bigpot/system"
)

var ClassRelId system.Oid = 1259
var ClassTupleDesc = &TupleDesc{
	Attrs: []*Attribute{
		{"relname", system.NameType},
		{"relfilenode", system.OidType},
	},
	typid: ClassRelId,
	hasOid: true,
}

const (
	Anum_class_relname     = 1
	Anum_clasS_relfilenode = 2
)

var AttributeRelId system.Oid = 1249
var AttributeTupleDesc = &TupleDesc{
	Attrs: []*Attribute{
		{"attrelid", system.OidType},
		{"attname", system.NameType},
		{"attnum", system.Int4Type},
		{"atttypid", system.OidType},
	},
	typid: AttributeRelId,
	hasOid: false,
}

const (
	Anum_attribute_attrelid = 1
	Anum_attribute_attname  = 2
	Anum_attribute_attnum   = 3
	Anum_attribute_atttypid = 4
)
