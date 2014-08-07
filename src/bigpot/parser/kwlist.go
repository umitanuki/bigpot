package parser

import (
	"errors"
	"strings"
)

const (
	ReservedKeyword = iota
	TypeFuncNameKeyword
	ColNameKeyword
	UnreservedKeyword
)

type keyword struct {
	name   string
	token  int
	kwtype int
}

/*
 * We'd like to avoid dupliate definitions between gram.y and here,
 * but because we need our keyword type definition while yacc needs to know
 * the set of keywords at compile time.
 */
var keywordList = []keyword{
	{"from", FROM, ReservedKeyword},
	{"select", SELECT, ReservedKeyword},
}

func findKeyword(name string) (*keyword, error) {
	name = strings.ToLower(name)
	for _, kw := range keywordList {
		if kw.name == name {
			return &kw, nil
		}
	}
	return nil, errors.New("keyword not found")
}
