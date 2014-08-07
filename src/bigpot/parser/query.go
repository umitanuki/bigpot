package parser

import "errors"

//import "bigpot/relation"
import "bigpot/access"
import "bigpot/system"

type CommandType int

const (
	CMD_SELECT = CommandType(iota)
	CMD_INSERT
	CMD_UPDATE
	CMD_DELETE
)

type Alias struct {
	AliasName   string
	ColumnNames []string
}

type RangeVar struct {
	SchemaName   system.Name
	RelationName system.Name
	AliasName    Alias
}

type Expr interface {
	ResultType() system.Oid
}

type ExprImpl struct {
	resultType system.Oid
}

type Var struct {
	ExprImpl
	// TODO: AttributeId is int32?
	VarNo    uint16
	VarAttNo uint16
}

type TargetEntry struct {
	Expr    Expr
	ResNo   uint16
	ResName system.Name
	ResJunk bool
}

type RteType int

const (
	RTE_RELATION = RteType(iota)
	RTE_SUBQUERY
	RTE_JOIN
	RTE_VALUES
	RTE_FUNCTION
	RTE_CTE
)

type RangeTblEntry struct {
	RteType RteType
	/* for relation */
	RelId    system.Oid
	RefAlias *Alias
}

type Query struct {
	CommandType CommandType
	TargetList  []*TargetEntry
	RangeTables []*RangeTblEntry
}

type Parser interface {
	Parse(query_string string) *Query
}

type ParserImpl struct {
	query     string
	namespace []*RangeTblEntry
}

type ParserError struct {
	msg      string
	location int
}

func (e ParserError) Error() string {
	return e.msg
}

func parseError(msg string) error {
	/* TODO: get stack */
	return ParserError{msg: msg}
}

func (parser *ParserImpl) Parse(query_string string) (*Query, error) {
	lexer := newLexer(query_string)
	yyParse(lexer)
	node := TopList[0]
	return parser.transformStmt(node)
}

func (parser *ParserImpl) transformStmt(node Node) (*Query, error) {
	switch node.(type) {
	default:
		return nil, parseError("unknown node type")
	case *SelectStmt:
		return parser.transformSelectStmt(node.(*SelectStmt))
	}
	panic("unreachable")
}

func (parser *ParserImpl) transformSelectStmt(stmt *SelectStmt) (query *Query, err error) {
	query = &Query{CommandType: CMD_SELECT}
	err = nil
	if err = parser.transformFromClause(stmt); err != nil {
		return
	}
	if query.TargetList, err =
		parser.transformTargetList(stmt.targetList); err != nil {
		return
	}

	query.RangeTables = parser.namespace

	return
}

func (parser *ParserImpl) transformFromClause(stmt *SelectStmt) error {
	for _, item := range stmt.fromList {
		switch item.(type) {
		default:
			return parseError("unknown node type")
		case *RangeVar:
			rv := item.(*RangeVar)
			rte := &RangeTblEntry{}
			relation, err := rv.OpenRelation()
			if err != nil {
				return err
			}
			defer relation.Close()
			rte.RelId = relation.RelId
			rte.RefAlias = buildAlias(relation)

			parser.namespace = append(parser.namespace, rte)
		}
	}

	return nil
}

func buildAlias(relation *access.Relation) *Alias {
	alias := &Alias{}
	/* TODO: Add check for user-provided alias name */
	alias.AliasName = string(relation.RelName)

	names := []string{}
	for _, attr := range relation.RelDesc.Attrs {
		names = append(names, string(attr.AttName))
	}
	alias.ColumnNames = names

	return alias
}

func (parser *ParserImpl) transformTargetList(targetList []*ResTarget) (tlist []*TargetEntry, err error) {
	for i, item := range targetList {
		var tle *TargetEntry
		tle, err = parser.transformTargetEntry(item)
		tle.ResNo = uint16(i + 1)
		if err != nil {
			return
		}
		tlist = append(tlist, tle)
	}

	return
}

func (parser *ParserImpl) transformTargetEntry(restarget *ResTarget) (tle *TargetEntry, err error) {
	tle = &TargetEntry{}
	err = nil

	tle.Expr, err = parser.transformExpr(restarget.val)
	/* ResName, ResNo */
	tle.ResName = system.Name(restarget.name)
	tle.ResJunk = false

	return
}

func (parser *ParserImpl) transformExpr(node Node) (expr Expr, err error) {
	switch node.(type) {
	default:
		return nil, parseError("unknown node type")
	case *ColumnRef:
		colref := node.(*ColumnRef)
		/* TODO: use map instead of linear search? */
		found := false
		var variable Var
		for rteidx, rte := range parser.namespace {
			/* TODO: colref may have table name too */
			for attidx, attname := range rte.RefAlias.ColumnNames {
				if attname == colref.name {
					if found {
						return nil, parseError("ambiguous column reference")
					}
					found = true
					relation, err := access.HeapOpen(rte.RelId)
					if err != nil {
						return nil, err
					}
					tupdesc := relation.RelDesc

					variable.resultType = tupdesc.Attrs[attidx].AttType
					variable.VarNo = uint16(rteidx + 1)
					variable.VarAttNo = uint16(attidx + 1)
				}
			}

		}
		if !found {
			return nil, parseError("column reference not found")
		}

		return Expr(&variable), nil

	}

	panic("unreachable")
}

func (rv *RangeVar) OpenRelation() (rel *access.Relation, err error) {
	/* TODO: Refactor this to RelnameGetOid() */
	class_rel, err := access.HeapOpen(access.ClassRelId)
	if err != nil {
		return nil, err
	}
	defer class_rel.Close()
	scankeys := []access.ScanKey{
		{access.Anum_class_relname, system.Datum(rv.RelationName)},
	}
	scan, err := class_rel.BeginScan(scankeys)
	if err != nil {
		return nil, err
	}
	defer scan.EndScan()
	tuple, err := scan.Next()
	if err != nil {
		return nil, errors.New("relation not found")
	}
	relid := tuple.Get(int32(1)).(system.Oid)

	return access.HeapOpen(relid)
}

func (node *ExprImpl) ResultType() system.Oid {
	return node.resultType
}
