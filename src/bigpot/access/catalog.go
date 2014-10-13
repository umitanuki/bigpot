package access

import (
	"bigpot/system"
)

var ClassTupleDesc = &TupleDesc{
	[]*Attribute{
		{"relname", system.NameType},
		{"relfilenode", system.OidType},
	},
}
var ClassRelId system.Oid = 1259

const (
	Anum_class_relname     = 1
	Anum_clasS_relfilenode = 2
)

var AttributeTupleDesc = &TupleDesc{
	[]*Attribute{
		{"attrelid", system.OidType},
		{"attname", system.NameType},
		{"attnum", system.Int4Type},
		{"atttypid", system.OidType},
	},
}
var AttributeRelId system.Oid = 1249

const (
	Anum_attribute_attrelid = 1
	Anum_attribute_attname  = 2
	Anum_attribute_attnum   = 3
	Anum_attribute_atttypid = 4
)
