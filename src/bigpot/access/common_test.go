package access

import (
	"bigpot/system"
	"fmt"
	. "launchpad.net/gocheck"
	"testing"
)

func init() {
	DatabaseDir = "testdata"
}

func Test(t *testing.T) {
	TestingT(t)
}

type AccessSuite struct{}

var _ = Suite(&AccessSuite{})

func (s *AccessSuite) TestHeapOpen_class(c *C) {
	relation, err := HeapOpen(ClassRelId)
	if err != nil {
		c.Error(err)
	}
	c.Check(relation.RelId, Equals, ClassRelId)
	c.Check(len(relation.RelDesc.Attrs), Equals, 2)
}

func (s *AccessSuite) TestHeapOpen_attribute(c *C) {
	relation, err := HeapOpen(AttributeRelId)
	if err != nil {
		c.Error(err)
	}
	c.Check(relation.RelId, Equals, AttributeRelId)
	c.Check(len(relation.RelDesc.Attrs), Equals, 4)
}

func ExampleHeapScan_class() {
	relation, err := HeapOpen(ClassRelId)
	if err != nil {
		fmt.Printf("%s", err)
		return
	}
	defer relation.Close()

	scan, err := relation.BeginScan(nil)
	if err != nil {
		fmt.Printf("%s", err)
		return
	}

	defer scan.EndScan()
	for i := 1; ; i++ {
		tuple, err := scan.Next()
		if err != nil {
			break
		}
		for j := 1; j <= len(relation.RelDesc.Attrs); j++ {
			fmt.Printf("%d,%d:%s\n", i, j, tuple.Get(int32(j)).ToString())
		}
	}

	// OUTPUT:
	// 1,1:1259
	// 1,2:bp_class
	// 2,1:1249
	// 2,2:bp_attribute
	// 3,1:20109
	// 3,2:test_table
}

func ExampleHeapScan_usertable() {
	relation, err := HeapOpen(system.Oid(20109))
	if err != nil {
		fmt.Printf("%s", err)
		return
	}
	defer relation.Close()

	scan, err := relation.BeginScan(nil)
	if err != nil {
		fmt.Printf("%s", err)
		return
	}

	defer scan.EndScan()
	for i := 1; ; i++ {
		tuple, err := scan.Next()
		if err != nil {
			break
		}
		for j := 1; j <= len(relation.RelDesc.Attrs); j++ {
			fmt.Printf("%d,%d:%s\n", i, j, tuple.Get(int32(j)).ToString())
		}
	}

	// OUTPUT:
	// 1,1:1
	// 1,2:foo1
	// 2,1:2
	// 2,2:bar2
	// 3,1:3
	// 3,2:aho3
	// 4,1:4
	// 4,2:bar5
}
