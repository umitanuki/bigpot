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

var _ = Suite(&MySuite{})

func (s *MySuite) TestDatumFromBytes(c *C) {
	var b []byte

	b = []byte{'a', 'b', 'c', 0x00, 'd'}
	b = append(b, make([]byte, NameLen-len(b))...)
	r := bytes.NewReader(b)
	name := DatumFromBytes(r, NameType)

	c.Check(name, Equals, Name("abc"))
}

func (s *MySuite) TestDatumFromStringOid(c *C) {

	oid1, err := DatumFromString("42", OidType)
	c.Check(oid1, Equals, Oid(42))

	_, err = DatumFromString("-1", OidType)
	c.Check(err.Error(), Equals, "invalid syntax")
}
