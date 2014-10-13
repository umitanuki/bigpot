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

	page.Init(0)

	c.Check(page.IsValid(), Equals, true)
	c.Check(page.IsEmpty(), Equals, true)
	c.Check(page.PageSize(), Equals, uint16(system.BlockSize))
	c.Check(page.PageLayourVersion(), Equals, uint16(4))

	page.SetLower(128)
	page.SetUpper(1024)
	c.Check(page.Lower(), Equals, uint16(128))
	c.Check(page.Upper(), Equals, uint16(1024))
	c.Check(page.IsEmpty(), Equals, false)

	c.Check(page.IsNew(), Equals, false)

	fout, _ := os.Create("/tmp/foo")
	fout.Write(b)
	fout.Close()

	fin, _ := os.Open("/tmp/foo")
	b2 := make([]byte, system.BlockSize)
	fin.Read(b2)
	fin.Close()
	page2 := NewPage(b2)

	c.Check(page2.PageSize(), Equals, uint16(system.BlockSize))
	c.Check(page2.PageLayourVersion(), Equals, uint16(4))

	c.Check(page2.Lower(), Equals, uint16(128))
	c.Check(page2.Upper(), Equals, uint16(1024))

	// Test for Add/Item
	// TODO: overwrite case
	page3 := NewPage(make([]byte, system.BlockSize))
	page3.Init(0)
	item1 := make([]byte, 128)
	item1[0] = 0xbe
	item1[1] = 0xde
	item2 := make([]byte, 256)
	item2[254] = 0xaa
	item2[255] = 0xab
	offset1 := page3.AddItem(item1, system.InvalidOffsetNumber, false, true)
	offset2 := page3.AddItem(item2, system.InvalidOffsetNumber, false, true)
	c.Check(offset1, Equals, system.OffsetNumber(1))
	c.Check(offset2, Equals, system.OffsetNumber(2))
	itemId1 := page3.ItemId(offset1)
	item1 = page3.Item(itemId1)
	c.Check(item1[0], Equals, byte(0xbe))
	c.Check(item1[1], Equals, byte(0xde))
	itemId2 := page3.ItemId(offset2)
	item2 = page3.Item(itemId2)
	c.Check(item2[254], Equals, byte(0xaa))
	c.Check(item2[255], Equals, byte(0xab))
}

func (s *MySuite) TestItemId(c *C) {
	itid := ItemId(0)

	itid.SetFlags(ItemIdUnused)
	itid.SetLength(42)
	itid.SetOffset(16)

	c.Check(itid.Flags(), Equals, ItemIdUnused)
	c.Check(itid.Length(), Equals, uint(42))
	c.Check(itid.Offset(), Equals, uint(16))

	itid.SetNormal(system.BlockSize - 128, 128)
	c.Check(itid.Flags(), Equals, ItemIdNormal)
	c.Check(itid.Length(), Equals, uint(128))
	c.Check(itid.Offset(), Equals, uint(system.BlockSize - 128))
}
