package system

import (
	"fmt"
)

const DefaultTableSpaceOid = 1663
const GlobalTableSpaceOid = 1664

type RelFileNode struct {
	Dbid  Oid
	Tsid  Oid
	Relid Oid
}

func RelPath(rnode RelFileNode) string {
	if rnode.Tsid == GlobalTableSpaceOid {
		return fmt.Sprintf("base/global/%d", rnode.Relid)
	} else if rnode.Tsid == DefaultTableSpaceOid {
		return fmt.Sprintf("base/%d/%d", rnode.Dbid, rnode.Relid)
	} else {
		panic("non-default tablespace is not supported")
	}
}
