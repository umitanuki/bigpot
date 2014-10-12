package storage

import (
	//"bytes"
	"os"
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

func (s *MySuite) TestPage(c *C) {
	b := make([]byte, system.BlockSize)
	page := NewPage(b)

	c.Check(page.IsNew(), Equals, true)

	page.SetLower(128)
	page.SetUpper(1024)
	c.Check(page.Lower(), Equals, uint16(128))
	c.Check(page.Upper(), Equals, uint16(1024))
	c.Check(page.IsEmpty(), Equals, false)

	page.SetLower(10)
	c.Check(page.IsEmpty(), Equals, true)

	c.Check(page.IsNew(), Equals, false)

	fout, _ := os.Create("/tmp/foo")
	fout.Write(b)
	fout.Close()

	fin, _ := os.Open("/tmp/foo")
	b2 := make([]byte, system.BlockSize)
	fin.Read(b2)
	fin.Close()
	page2 := NewPage(b2)

	c.Check(page2.Lower(), Equals, uint16(10))
	c.Check(page2.Upper(), Equals, uint16(1024))
}

func (s *MySuite) TestItemId(c *C) {
	itid := ItemId(0)

	itid.SetFlags(ItemIdUsed)
	itid.SetLength(42)
	itid.SetOffset(16)

	c.Check(itid.Flags(), Equals, ItemIdUsed)
	c.Check(itid.Length(), Equals, uint(42))
	c.Check(itid.Offset(), Equals, uint(16))
}
