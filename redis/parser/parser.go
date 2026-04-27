package parser

import (
	_interface "MiddlewareSelf/redis/interface"
	"MiddlewareSelf/redis/resp"
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"strconv"
)

const (
	MaxBulkLength  int64 = 512 * 1024 * 1024
	MaxArrayLength int64 = 1024 * 1024

	maxLineLength   = 64 * 1024
	arrayInitialCap = 16
)

var (
	errLineTooLarge = errors.New("protocol line too large")
	errMissingCRLF  = errors.New("missing CRLF")
)

type Payload struct {
	Data _interface.Reply
	Err  error
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse(reader, ch)
	return ch
}

func parse(rawReader io.Reader, ch chan *Payload) {
	defer close(ch)
	reader := bufio.NewReader(rawReader)
	for {
		line, err := readLine(reader)
		if err != nil {
			emitReadError(ch, err)
			return
		}
		if len(line) == 0 {
			ch <- &Payload{Err: errors.New("empty line")}
			continue
		}
		switch line[0] {
		case '+':
			ch <- &Payload{
				Data: resp.MakeSimpleReply(string(line[1:])),
			}
		case '-':
			ch <- &Payload{
				Data: resp.MakeErrorReply(string(line[1:])),
			}
		case ':':
			content, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				ch <- &Payload{Err: errors.New("::invalid parseInt")}
				return
			}
			ch <- &Payload{
				Data: resp.MakeIntegerReply(content),
			}
		case '$':
			if !parseBulk(reader, ch, line) {
				return
			}
		case '*':
			if !parseArray(reader, ch, line) {
				return
			}
		default:
			ch <- &Payload{Err: errors.New("error pattern.please write again")}
		}
	}
}

func readLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(fragment) > 0 {
			if len(line)+len(fragment) > maxLineLength {
				return nil, errLineTooLarge
			}
			line = append(line, fragment...)
		}
		if err == nil {
			if !bytes.HasSuffix(line, []byte{'\r', '\n'}) {
				return nil, errMissingCRLF
			}
			return line[:len(line)-2], nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return nil, err
	}
}

func emitReadError(ch chan<- *Payload, err error) {
	if err == io.EOF {
		ch <- &Payload{Err: errors.New("EOF")}
		return
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		ch <- &Payload{Err: errors.New("os.ErrDeadlineExceeded")}
		return
	}
	ch <- &Payload{Err: err}
}

func parseArray(reader *bufio.Reader, ch chan<- *Payload, header []byte) bool {
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < -1 {
		ch <- &Payload{Err: errors.New("invalid array format")}
		return false
	}
	if nStrs > MaxArrayLength {
		ch <- &Payload{Err: errors.New("array length too large")}
		return false
	}
	if nStrs == -1 {
		ch <- &Payload{
			Data: resp.MakeArrayReply(nil),
		}
		return true
	}
	if nStrs == 0 {
		ch <- &Payload{
			Data: resp.MakeArrayReply([][]byte{}),
		}
		return true
	}

	capacity := int(nStrs)
	if capacity > arrayInitialCap {
		capacity = arrayInitialCap
	}
	lines := make([][]byte, 0, capacity)
	for i := int64(0); i < nStrs; i++ {
		line, err := readLine(reader)
		if err != nil {
			ch <- &Payload{Err: errors.New("invalid array length")}
			return false
		}

		if len(line) < 2 || line[0] != '$' {
			ch <- &Payload{Err: errors.New("invalid array element header")}
			return false
		}

		strLen, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil || strLen < -1 {
			ch <- &Payload{Err: errors.New("invalid array bulk length")}
			return false
		}
		if strLen > MaxBulkLength {
			ch <- &Payload{Err: errors.New("array bulk length too large")}
			return false
		}

		if strLen == -1 {
			lines = append(lines, nil)
			continue
		}

		body, err := readBulkBody(reader, int(strLen))
		if err != nil {
			ch <- &Payload{Err: errors.New("invalid array parse")}
			return false
		}
		lines = append(lines, body)
	}
	ch <- &Payload{
		Data: resp.MakeArrayReply(lines),
	}
	return true
}

func parseBulk(reader *bufio.Reader, ch chan<- *Payload, line []byte) bool {
	strlen, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil || strlen < -1 {
		ch <- &Payload{Err: errors.New("invalid bulk length")}
		return false
	}
	if strlen > MaxBulkLength {
		ch <- &Payload{Err: errors.New("bulk string too large")}
		return false
	}

	if strlen == -1 {
		ch <- &Payload{
			Data: resp.MakeBulkReply(nil),
		}
		return true
	}

	strBuf, err := readBulkBody(reader, int(strlen))
	if err != nil {
		ch <- &Payload{Err: errors.New("invalid bulk parse")}
		return false
	}
	ch <- &Payload{
		Data: resp.MakeBulkReply(strBuf),
	}
	return true
}

func readBulkBody(reader *bufio.Reader, length int) ([]byte, error) {
	body := make([]byte, length+2)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, err
	}
	if !bytes.HasSuffix(body, []byte{'\r', '\n'}) {
		return nil, errMissingCRLF
	}
	return body[:length], nil
}
