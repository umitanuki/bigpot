package access

import (
	. "launchpad.net/gocheck"
	"testing"

	"bigpot/system"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestHeapTuple(c *C) {
	values := []system.Datum{
		system.Int4(1),
		system.Oid(999990),
		nil,
		system.Name("fooname"),
	}

	tupdesc := &TupleDesc{
		Attrs: []*Attribute{
			{
				Name:   "col1",
				TypeId: system.Int4Type,
			},
			{
				Name:   "col2",
				TypeId: system.OidType,
			},
			{
				Name:   "col3nil",
				TypeId: system.NameType,
			},
			{
				Name:   "col4",
				TypeId: system.NameType,
			},
		},
	}
	initTupleDesc(tupdesc)
	htuple := FormHeapTuple(values, tupdesc)
	c.Check(htuple.Fetch(1), Equals, values[0])
	c.Check(htuple.Fetch(2), Equals, values[1])
	c.Check(htuple.Fetch(3), Equals, values[2])
	c.Check(htuple.Fetch(4), Equals, values[3])
	c.Check(htuple.data.Oid(), Equals, system.InvalidOid)
	c.Check(htuple.data.HasNulls(), Equals, true)
	c.Check(htuple.data.Natts(), Equals, system.AttrNumber(4))
}
