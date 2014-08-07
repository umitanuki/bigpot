package planner

import (
	"bigpot/parser"
	//	"bigpot/system"
)

type Node interface {
}

type PlanRoot struct {
	CommandType parser.CommandType
	Plan        Node
	RangeTables []*parser.RangeTblEntry
}

type Planner interface {
	Plan(query parser.Query) *PlanRoot
}

type PlannerImpl struct {
}

type Plan struct {
	LeftTree  Node
	RightTree Node
}

type SeqScan struct {
	Plan
	TargetList []*parser.TargetEntry
	RangeTable *parser.RangeTblEntry
	rel        uint32
}

func (planner *PlannerImpl) Plan(query parser.Query) *PlanRoot {
	root := PlanRoot{}

	root.CommandType = query.CommandType

	root.Plan = Node(makeSeqScan(query.TargetList, query.RangeTables[0]))
	/* TODO: deep copy */
	root.RangeTables = query.RangeTables

	return &root
}

func makeSeqScan(tlist []*parser.TargetEntry, rte *parser.RangeTblEntry) *SeqScan {
	scan := &SeqScan{
		TargetList: tlist,
		RangeTable: rte,
	}

	return scan
}
