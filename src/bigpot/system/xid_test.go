package system

import (
	. "launchpad.net/gocheck"
)

func (s *MySuite) TestXid(c *C) {
	c.Check(Xid(1).IsValid(), Equals, true)
	c.Check(Xid(1).IsNormal(), Equals, false)

	c.Check(Xid(1).Precedes(Xid(999)), Equals, true)
	c.Check(Xid(2).Precedes(Xid(999)), Equals, true)
	c.Check(Xid(3).Precedes(Xid(0xFFFFFFFF)), Equals, false)
	c.Check(Xid(4).PrecedesOrEquals(Xid(4)), Equals, true)
	c.Check(Xid(2).PrecedesOrEquals(Xid(4)), Equals, true)
	c.Check(Xid(2).Precedes(Xid(0xFFFFFFFF)), Equals, true)

	c.Check(Xid(1).Follows(Xid(999)), Equals, false)
	c.Check(Xid(2).Follows(Xid(999)), Equals, false)
	c.Check(Xid(3).Follows(Xid(0xFFFFFFFF)), Equals, true)
	c.Check(Xid(4).FollowsOrEquals(Xid(4)), Equals, true)
	c.Check(Xid(2).FollowsOrEquals(Xid(4)), Equals, false)
	c.Check(Xid(2).Follows(Xid(0xFFFFFFFF)), Equals, false)

}
