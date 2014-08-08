package system

import (
	"fmt"
)

type ErrorCode [5]byte

var InvalidTextRepresentation = ErrorCode{'2', '2', 'P', '0', '2'}

var InternalError = ErrorCode{'X', 'X', '0', '0', '0'}

type Error struct {
	code ErrorCode
	msg  string
}

func Ereport(code ErrorCode, msg string, v ...interface{}) *Error {
	msgstr := fmt.Sprintf(msg, v...)
	return &Error{
		code: code,
		msg:  msgstr,
	}
}

func Elog(msg string, v ...interface{}) *Error {
	return Ereport(InternalError, msg, v...)
}

func (e *Error) Error() string {
	return e.msg
}
