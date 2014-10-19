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

	// For now, create an empty file first.
	file, err := os.Create("base/1/1259")
	file.Close()

	// Test read a block with extend, release, and read it again.
	buf, err := mgr.ReadBuffer(reln, NewBlock)
	c.Assert(err, Equals, nil)
	page := buf.GetPage()
	page.Init(0)
	buf.MarkDirty()
	c.Check(page.IsNew(), Equals, false)
	mgr.ReleaseBuffer(buf)

	// hard code "0"
	buf2, err := mgr.ReadBuffer(reln, 0)
	c.Assert(err, Equals, nil)
	page2 := buf2.GetPage()
	c.Check(page2.IsEmpty(), Equals, true)
	c.Check(page2.IsNew(), Equals, false)

	// Test using all buffers
	for i := 0; i < 16; i++ {
		buf, err := mgr.ReadBuffer(reln, NewBlock)
		c.Assert(err, Equals, nil)
		mgr.ReleaseBuffer(buf)
	}

	// Make sure the previous read buffer doesn't go away
	c.Check(page2.IsNew(), Equals, false)
}
