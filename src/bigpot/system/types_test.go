package system

import (
	"bytes"
	. "launchpad.net/gocheck"
	"testing"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type MySuite struct{}
var _ =Suite(&MySuite{})

func (s *MySuite) TestDatumFromString(c *C) {
	var b []byte

	b = []byte{'a', 'b', 'c', 0x00, 'd'}
	b = append(b, make([]byte, NameLen - len(b))...)
	r := bytes.NewReader(b)
	name := DatumFromBytes(r, NameType)

	c.Check(name, Equals, Name("abc"))
}
