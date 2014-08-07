package parser

import (
	"fmt"
)

func (l *lexer) lexPrintExpect(expected int) {
	lval := &yySymType{}
	if token := l.Lex(lval); token != expected {
		fmt.Printf("not expected token: %d != %d\n", token, expected)
		return
	}
	switch expected {
	default:
		if expected > 0 && expected < 127 {
			fmt.Printf("%c\n", expected)
		} else if expected > COLON_EQUALS { /* too hacky... */
			fmt.Printf("%s\n", lval.keyword)
		} else {
			fmt.Printf("%s\n", lval.str)
		}
	case ICONST, PARAM:
		fmt.Printf("%d\n", lval.ival)
	}
}

func recoverPanic() {
	if err := recover(); err != nil {
		fmt.Println("failed:", err)
	}
}

func ExampleLex_1() {
	lexer := newLexer("select 1 -- comment_comment\n2")
	lexer.lexPrintExpect(SELECT)
	lexer.lexPrintExpect(ICONST)
	lexer.lexPrintExpect(ICONST)
	// Output:
	// select
	// 1
	// 2
}

func ExampleLex_2() {
	lexer := newLexer("select col1, col2 from tab1")
	lexer.lexPrintExpect(SELECT)
	lexer.lexPrintExpect(IDENT)
	lexer.lexPrintExpect(int(','))
	lexer.lexPrintExpect(IDENT)
	lexer.lexPrintExpect(FROM)
	lexer.lexPrintExpect(IDENT)
	// Output:
	// select
	// col1
	// ,
	// col2
	// from
	// tab1
}

func ExampleLex_consts() {
	lexer := newLexer("b'0101' x'ff'")
	lexer.lexPrintExpect(BCONST)
	lexer.lexPrintExpect(XCONST)
	// Output:
	// b0101
	// xff
}

func ExampleLex_sconsts() {
	lexer := newLexer("select 'foo'  /* comment /* c2 */ */ bar " +
		"$$lex$$ $body$text$body$  $a$sentence$c$a$ ")
	lexer.lexPrintExpect(SELECT)
	lexer.lexPrintExpect(SCONST)
	lexer.lexPrintExpect(IDENT)
	lexer.lexPrintExpect(SCONST)
	lexer.lexPrintExpect(SCONST)
	lexer.lexPrintExpect(SCONST)
	// Output:
	// select
	// foo
	// bar
	// lex
	// text
	// sentence$c
}

func ExampleLex_numbers() {
	lexer := newLexer("10 0.1e  1.53e-1 1.  0001.999 9999999999999999999")
	lexer.lexPrintExpect(ICONST)
	lexer.lexPrintExpect(FCONST)
	lexer.lexPrintExpect(IDENT)
	lexer.lexPrintExpect(FCONST)
	lexer.lexPrintExpect(FCONST)
	lexer.lexPrintExpect(FCONST)
	lexer.lexPrintExpect(FCONST)
	// Output:
	// 10
	// 0.1
	// e
	// 1.53e-1
	// 1.
	// 0001.999
	// 9999999999999999999
}

func ExampleLex_operators() {
	lexer := newLexer("1 % 2 -/* foo*/10 <> !=")
	lexer.lexPrintExpect(ICONST)
	lexer.lexPrintExpect(int('%'))
	lexer.lexPrintExpect(ICONST)
	lexer.lexPrintExpect(int('-'))
	lexer.lexPrintExpect(ICONST)
	lexer.lexPrintExpect(Op)
	lexer.lexPrintExpect(Op)
	// Output:
	// 1
	// %
	// 2
	// -
	// 10
	// <>
	// <>
}

func ExampleLex_params() {
	lexer := newLexer("$1 $0")
	lexer.lexPrintExpect(PARAM)
	lexer.lexPrintExpect(PARAM)
	// Output:
	// 1
	// 0
}

func ExampleLex_negative1() {
	defer recoverPanic()
	lexer := newLexer("select /* comment")
	lval := &yySymType{}
	lexer.Lex(lval)
	lexer.Lex(lval)
	// Output: failed: unterminated /* comment
}

func ExampleLex_params_negative() {
	defer recoverPanic()
	lexer := newLexer("$99999999999999999999999999999")
	lexer.lexPrintExpect(PARAM)
	// Output: failed: value out of range for param
}

func ExampleLex_xb_negative() {
	defer recoverPanic()
	lexer := newLexer("b'01010")
	lexer.lexPrintExpect(BCONST)
	// Output: failed: unterminated bit string literal
}

func ExmapleLex_xh_negative() {
	defer recoverPanic()
	lexer := newLexer("x'ffee")
	lexer.lexPrintExpect(XCONST)
	// Output: failed: unterminated hexadecimal string literal
}

func ExampleLex_dolq_ngative() {
	defer recoverPanic()
	lexer := newLexer("$$abcd")
	lexer.lexPrintExpect(SCONST)
	// Output: failed: unterminated dollar-quoted string
}

func ExampleLex_xd_negative() {
	defer recoverPanic()
	lexer := newLexer("\"abcd")
	lexer.lexPrintExpect(IDENT)
	// Output: failed: unterminated quoted identifier
}
