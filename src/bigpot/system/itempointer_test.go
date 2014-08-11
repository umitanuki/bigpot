package system

import (
	. "launchpad.net/gocheck"
)

func (s *MySuite) TestItemPointerDatumFromString(c *C) {
	var str string

	str = "(1,30)"
	item1, err := DatumFromString(str, TidType)
	c.Check(item1, DeepEquals, &ItemPointer{1, 30})

	str = "(1,)"
	_, err = DatumFromString(str, TidType)
	c.Check(err.Error(), Equals, "invalid syntax for tid")
}
