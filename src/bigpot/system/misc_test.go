package system

import (
	. "launchpad.net/gocheck"
)

func (s *MySuite) TestMaxAlign(c *C) {
	c.Check(TypeAlign(8, 15), Equals, uintptr(16))
}
