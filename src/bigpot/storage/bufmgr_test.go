package storage

import (
	. "launchpad.net/gocheck"
	"os"

	"bigpot/system"
)

func (s *MySuite) TestBufferManager(c *C) {
	os.MkdirAll("base/1", 0700)
	defer os.RemoveAll("base")

	mgr := NewBufferManager(16)

	reln := system.RelFileNode{1, system.DefaultTableSpaceOid, 1259}
	_, err := mgr.ReadBuffer(reln, NewBlock)
	c.Check(err, ErrorMatches, ".* no such file or directory")

	file, err := os.Create("base/1/1259")
	file.Close()

	buf, err := mgr.ReadBuffer(reln, NewBlock)
	page := buf.GetPage()
	page.Init(0)
	c.Check(page.IsNew(), Equals, false)
	mgr.ReleaseBuffer(buf)

	buf2, err := mgr.ReadBuffer(reln, 0)
	c.Check(err, Equals, nil)
	page2 := buf2.GetPage()
	c.Check(page2.IsEmpty(), Equals, true)
	c.Check(page2.IsNew(), Equals, false)
}
