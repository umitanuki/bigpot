package parser

import (
	. "launchpad.net/gocheck"
	//	"testing"
	"bigpot/access"
	"bigpot/system"
)

func init() {
	access.DatabaseDir = "../access/testdata"
}

var _ = Suite(&MySuite{})

func (s *MySuite) TestBuildAlias(c *C) {
	relation := access.Relation{
		RelName: "mytable",
		RelDesc: &access.TupleDesc{
			Attrs: []*access.Attribute{
				{"mycol1", system.NameType},
				{"mycol2", system.Int4Type},
			},
		},
	}

	alias := buildAlias(&relation)
	c.Check(alias.AliasName, Equals, "mytable")
	c.Check(len(alias.ColumnNames), Equals, 2)
	c.Check(alias.ColumnNames[0], Equals, "mycol1")
	c.Check(alias.ColumnNames[1], Equals, "mycol2")
}

func (s *MySuite) TestTransform(c *C) {
	parser := ParserImpl{}
	query, err := parser.Parse("select oid, relname from bp_class")
	if err != nil {
		c.Error(err)
	}

	c.Check(query.TargetList[0].Expr.(*Var).VarAttNo, Equals, uint16(1))
	c.Check(query.TargetList[0].Expr.(*Var).resultType, Equals, system.OidType)
	c.Check(query.TargetList[1].Expr.(*Var).VarAttNo, Equals, uint16(2))
	c.Check(query.TargetList[1].Expr.(*Var).resultType, Equals, system.NameType)
	c.Check(query.RangeTables[0].RteType, Equals, RTE_RELATION)
	c.Check(query.RangeTables[0].RelId, Equals, access.ClassRelId)
}
