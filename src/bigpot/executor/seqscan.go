package executor

import "bigpot/access"
import "bigpot/parser"
import "bigpot/planner"
import "bigpot/system"

type Node interface {
	Init()
	Exec() access.Tuple
	End()
}

type Scan interface {
	GetNext() access.Tuple
}

type SeqScan struct {
	planner.SeqScan
	relation   *access.Relation
	scan       *access.RelationScan
	executor   *ExecutorImpl
	targetDesc *access.TupleDesc
}

func (scan *SeqScan) Init() {
	var err error
	scan.relation, err = access.HeapOpen(scan.RangeTable.RelId)
	if err != nil {
		/* TODO: do stuff */
	}
	emptykeys := []access.ScanKey{}
	scan.scan, err = scan.relation.BeginScan(emptykeys)
	if err != nil {
		/* TODO: do stuff */
	}

	/* Build the output tuple desc */
	attrs := make([]*access.Attribute, len(scan.SeqScan.TargetList))
	for i, tle := range scan.SeqScan.TargetList {
		attrs[i] = &access.Attribute{
			tle.ResName, tle.Expr.ResultType(),
		}
	}
}

func (scan *SeqScan) Exec() access.Tuple {
	/* TODO: projection */
	tuple := scan.GetNext()
	scan.executor.scanTuple = tuple
	projected := scan.executor.projection(scan.SeqScan.TargetList,
		scan.targetDesc)
	return projected
}

func (scan *SeqScan) GetNext() access.Tuple {
	if tuple, err := scan.scan.Next(); err == nil {
		return tuple
	}

	return nil
}

func (scan *SeqScan) End() {
	scan.scan.EndScan()
	scan.relation.Close()
}

// --- will be moved elsewhere

func (executor *ExecutorImpl) projection(tlist []*parser.TargetEntry,
	tlistDesc *access.TupleDesc) access.Tuple {
	values := make([]string, len(tlist))
	for i, tle := range tlist {
		values[i] = executor.ExecExpr(tle.Expr).ToString()
	}

	return access.Tuple(&access.CSVTuple{
		TupleDesc: tlistDesc,
		Values:    values,
	})
}

func (executor *ExecutorImpl) ExecExpr(expr parser.Expr) system.Datum {
	switch expr.(type) {
	case *parser.Var:
		variable := expr.(*parser.Var)
		tuple := executor.scanTuple
		/* TODO: remove int32 */
		return tuple.Get(int32(variable.VarAttNo))
	}

	panic("unreachable")
}
