package executor

import "fmt"
import "bigpot/access"
import "bigpot/planner"

type Executor interface {
	Start()
	Execute()
	End()
}

type ExecutorImpl struct {
	planRoot  *planner.PlanRoot
	TupleDesc *access.TupleDesc
	execRoot  Node
	scanTuple access.Tuple
}

func NewExecutor(planRoot *planner.PlanRoot) *ExecutorImpl {
	return &ExecutorImpl{
		planRoot: planRoot,
	}
}

func (exec *ExecutorImpl) initExecNode(node planner.Node) Node {
	switch node.(type) {
	case *planner.SeqScan:
		scan := &SeqScan{}
		scan.SeqScan = *(node.(*planner.SeqScan))
		scan.executor = exec
		scan.Init()

		return Node(scan)
	}
	panic("unknown node type")
}

func (exec *ExecutorImpl) Start() {
	exec.execRoot = exec.initExecNode(exec.planRoot.Plan)
}

func (exec *ExecutorImpl) Execute() {
	for {
		tuple := exec.execRoot.Exec()
		if tuple == nil {
			break
		}
		/* TODO: Receiver */
		for attnum, _ := range exec.TupleDesc.Attrs {
			datum := tuple.Get(int32(attnum + 1))
			fmt.Printf("%s ", datum.ToString())
		}
		fmt.Println("")
	}
}

func (exec *ExecutorImpl) End() {
	exec.execRoot.End()
}
