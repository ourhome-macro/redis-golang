package resp

import (
	"bytes"
	"strconv"
)

var (
	CRLF = "\r\n"
)

// + string
type SimpleReply struct {
	Status string
}

func MakeSimpleReply(status string) *SimpleReply {
	return &SimpleReply{
		Status: status,
	}
}

func (reply *SimpleReply) ToBytes() []byte {
	return []byte("+" + reply.Status + CRLF)
}

// -Error
type ErrorReply struct {
	Error string
}

func MakeErrorReply(error string) *ErrorReply {
	return &ErrorReply{
		Error: error,
	}
}

func (reply *ErrorReply) ToBytes() []byte {
	return []byte("-" + reply.Error + CRLF)
}

// : int
type IntegerReply struct {
	code int64
}

func MakeIntegerReply(code int64) *IntegerReply {
	return &IntegerReply{
		code: code,
	}
}
func (reply *IntegerReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(reply.code, 10) + CRLF)
}

// $ string
type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return []byte("$-1" + CRLF)
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

// * multi
type ArrayReply struct {
	Args [][]byte
}

func MakeArrayReply(args [][]byte) *ArrayReply {
	return &ArrayReply{
		Args: args,
	}
}

func (r *ArrayReply) ToBytes() []byte {
	if r.Args == nil {
		return []byte("*-1" + CRLF)
	}

	var buf bytes.Buffer
	buf.Grow(len(r.Args) * 16)

	if len(r.Args) == 0 {
		return []byte("*0" + CRLF)
	}

	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(len(r.Args)))
	buf.WriteString(CRLF)

	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1")
			buf.WriteString(CRLF)
		} else {
			buf.WriteString("$")
			buf.WriteString(strconv.Itoa(len(arg)))
			buf.WriteString(CRLF)
			buf.Write(arg)
			buf.WriteString(CRLF)
		}
	}

	return buf.Bytes()
}
