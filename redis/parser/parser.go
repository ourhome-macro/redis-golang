package parser

import (
	_interface "MiddlewareSelf/redis/interface"
	"MiddlewareSelf/redis/resp"
	"bufio"
	"bytes"
	"io"
	"strconv"
	//"strings"
)

type Payload struct {
	Data _interface.Reply
	Err  error
}

//resp解析器
//＋ - ERROR . $ *

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse(reader, ch)
	return ch
}

func parse(rawReader io.Reader, ch chan *Payload) {
	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}
			close(ch)
			return
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		switch line[0] {
		case '+':
			content := string(line[1:])
			//content = string(content)
			ch <- &Payload{
				Data: resp.MakeSimpleReply(content),
			}
		case '-':
			content := string(line[1:])
			//content = string(content)
			ch <- &Payload{
				Data: resp.MakeErrorReply(content),
			}
		case ':':
			content, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
			ch <- &Payload{
				Data: resp.MakeIntegerReply(content),
			}
		case '$':
			parseBulk(reader, ch, line)
		case '*':
			parseArray(reader, ch, line)
		}
	}
}

func parseArray(reader *bufio.Reader, ch chan<- *Payload, header []byte) {
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < -1 {
		return
	} else if nStrs == -1 {
		// Null Array
		ch <- &Payload{
			Data: resp.MakeArrayReply(nil), // 确保你的 resp 包里有这个方法
		}
		return
	} else if nStrs == 0 {
		// Empty Array
		ch <- &Payload{
			Data: resp.MakeArrayReply([][]byte{}), // 空切片
		}
		return
	}

	lines := make([][]byte, 0, int(nStrs))
	for i := int64(0); i < nStrs; i++ {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}
			return
		}

		// 简单检查格式，必须以 $ 开头
		if len(line) < 2 || line[0] != '$' {
			return
		}

		strLenStr := string(line[1 : len(line)-2])
		strLen, err := strconv.ParseInt(strLenStr, 10, 64)

		if err != nil || strLen < -1 {
			return
		}

		if strLen == -1 {
			lines = append(lines, nil)
		} else {
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)
			if err != nil {
				ch <- &Payload{Err: err}
				return
			}
			lines = append(lines, body[:strLen])
		}
	}
	ch <- &Payload{
		Data: resp.MakeArrayReply(lines),
	}
}

func parseBulk(reader *bufio.Reader, ch chan *Payload, line []byte) {
	strlen, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil {
		ch <- &Payload{Err: err}
		return
	}
	strBuf := make([]byte, strlen+2)
	_, err = io.ReadFull(reader, strBuf)
	if err != nil {
		ch <- &Payload{Err: err}
		return
	}
	ch <- &Payload{
		Data: resp.MakeBulkReply(strBuf[:strlen]),
	}
}
