package parser

import (
	"bytes"
	"testing"
	"time"

	_interface "MiddlewareSelf/redis/interface"
	"MiddlewareSelf/redis/resp"
)

func TestParseStream_Real(t *testing.T) {
	// 构造一个复杂的 Redis 协议流
	// +OK, -Error, :123, $5hello, *2(foo, bar), $-1, *-1
	input := []byte("+OK\r\n-ERR error\r\n:123\r\n$5\r\nhello\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$-1\r\n*-1\r\n")

	ch := ParseStream(bytes.NewReader(input))

	// 辅助函数：从 channel 接收数据，带超时
	receive := func(timeout time.Duration) (*Payload, bool) {
		select {
		case payload := <-ch:
			return payload, true
		case <-time.After(timeout):
			return nil, false
		}
	}

	// 1. Test +OK
	if p, ok := receive(100 * time.Millisecond); ok {
		assertReplyEqual(t, p.Data, resp.MakeSimpleReply("OK"))
	} else {
		t.Fatal("Timeout waiting for +OK")
	}

	// 2. Test -Error
	if p, ok := receive(100 * time.Millisecond); ok {
		assertReplyEqual(t, p.Data, resp.MakeErrorReply("ERR error"))
	} else {
		t.Fatal("Timeout waiting for Error")
	}

	// 3. Test :123
	if p, ok := receive(100 * time.Millisecond); ok {
		assertReplyEqual(t, p.Data, resp.MakeIntegerReply(123))
	} else {
		t.Fatal("Timeout waiting for Integer")
	}

	// 4. Test $5 hello
	if p, ok := receive(100 * time.Millisecond); ok {
		assertReplyEqual(t, p.Data, resp.MakeBulkReply([]byte("hello")))
	} else {
		t.Fatal("Timeout waiting for Bulk")
	}

	// 5. Test *2
	if p, ok := receive(100 * time.Millisecond); ok {
		assertReplyEqual(t, p.Data, resp.MakeArrayReply([][]byte{[]byte("foo"), []byte("bar")}))
	} else {
		t.Fatal("Timeout waiting for Array")
	}

	// 6. Test $-1 (Null Bulk)
	if p, ok := receive(100 * time.Millisecond); ok {
		assertReplyEqual(t, p.Data, resp.MakeBulkReply(nil))
	} else {
		t.Fatal("Timeout waiting for Null Bulk")
	}

	// 7. Test *-1 (Null Array)
	if p, ok := receive(100 * time.Millisecond); ok {
		assertReplyEqual(t, p.Data, resp.MakeArrayReply(nil))
	} else {
		t.Fatal("Timeout waiting for Null Array")
	}
}

func assertReplyEqual(t *testing.T, actual, expected _interface.Reply) {
	if actual == nil {
		t.Errorf("Actual reply is nil")
		return
	}
	if expected == nil {
		t.Errorf("Expected reply is nil")
		return
	}

	aBytes := actual.ToBytes()
	eBytes := expected.ToBytes()

	if !bytes.Equal(aBytes, eBytes) {
		t.Errorf("Reply mismatch:\nExpected: %q\nActual:   %q", string(eBytes), string(aBytes))
	}
}
