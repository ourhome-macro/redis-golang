package client

import (
	_interface "MiddlewareSelf/redis/interface"
	"MiddlewareSelf/redis/resp"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// Command 表示一条 Redis 命令（RESP Array of Bulk Strings）。
// 例如：SET k v => [][]byte{[]byte("SET"), []byte("k"), []byte("v")}
type Command struct {
	Args [][]byte
}

// NewCommand 是便捷构造函数（字符串参数版）。
func NewCommand(args ...string) Command {
	b := make([][]byte, 0, len(args))
	for _, arg := range args {
		b = append(b, []byte(arg))
	}
	return Command{Args: b}
}

// PipelineResult 表示一条响应结果。
// - 成功：Err=nil 且 Reply!=nil
// - 失败：Err!=nil，同时 ResponseIndex 指示失败发生在第几条响应
type PipelineResult struct {
	ResponseIndex int
	Reply         _interface.Reply
	Err           error
	SuccessBefore int
}

// PipelineClient 是可复用的单连接 Pipeline 客户端。
// 当前实现每次仅允许一个 ExecStream 在同一连接上执行，避免响应错位。
type PipelineClient struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

	mu     sync.Mutex
	closed bool
}

// DialPipeline 建立 TCP 连接并启用 TCP_NODELAY（关闭 Nagle 算法）。
func DialPipeline(address string, timeout time.Duration) (*PipelineClient, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("set TCP_NODELAY failed: %w", err)
		}
	}

	return &PipelineClient{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, 32*1024),
		writer: bufio.NewWriterSize(conn, 32*1024),
	}, nil
}

// Close 关闭连接。
func (p *PipelineClient) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true
	return p.conn.Close()
}

// ExecStream 执行 Pipeline，并把每一条响应流式返回到 channel。
//
// 关键行为：
// 1) 发送端：边编码边写入 bufio.Writer，不拼巨大总包；
// 2) 接收端：底层 socket 循环中读到一个完整 RESP 就立刻向上游投递；
// 3) 错误定位：若第 N 条响应读取失败，会返回明确错误并带上前 N-1 条成功计数。
func (p *PipelineClient) ExecStream(ctx context.Context, commands []Command) (<-chan PipelineResult, error) {
	if len(commands) == 0 {
		return nil, fmt.Errorf("empty pipeline commands")
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("pipeline client is closed")
	}

	resultCh := make(chan PipelineResult, 16)

	go func() {
		defer p.mu.Unlock()
		defer close(resultCh)

		if deadline, ok := ctx.Deadline(); ok {
			_ = p.conn.SetDeadline(deadline)
			defer func() {
				_ = p.conn.SetDeadline(time.Time{})
			}()
		}

		// 1) 流式发送：逐条编码、逐条写入（并按批 flush）。
		for i, cmd := range commands {
			select {
			case <-ctx.Done():
				resultCh <- PipelineResult{
					ResponseIndex: i + 1,
					Err:           fmt.Errorf("pipeline canceled before sending command #%d: %w", i+1, ctx.Err()),
					SuccessBefore: i,
				}
				return
			default:
			}

			if err := writeCommand(p.writer, cmd.Args); err != nil {
				resultCh <- PipelineResult{
					ResponseIndex: i + 1,
					Err:           fmt.Errorf("pipeline write failed at command #%d: %w", i+1, err),
					SuccessBefore: i,
				}
				return
			}

			// 减少系统调用，同时避免无限积压。
			if (i+1)%64 == 0 {
				if err := p.writer.Flush(); err != nil {
					resultCh <- PipelineResult{
						ResponseIndex: i + 1,
						Err:           fmt.Errorf("pipeline flush failed after command #%d: %w", i+1, err),
						SuccessBefore: i,
					}
					return
				}
			}
		}

		if err := p.writer.Flush(); err != nil {
			resultCh <- PipelineResult{
				ResponseIndex: 1,
				Err:           fmt.Errorf("pipeline final flush failed: %w", err),
				SuccessBefore: 0,
			}
			return
		}

		// 2) 流式接收：每解析出一条完整 RESP，立刻返回。
		for i := 0; i < len(commands); i++ {
			select {
			case <-ctx.Done():
				resultCh <- PipelineResult{
					ResponseIndex: i + 1,
					Err:           fmt.Errorf("pipeline canceled while waiting response #%d: %w", i+1, ctx.Err()),
					SuccessBefore: i,
				}
				return
			default:
			}

			reply, err := readOneReply(p.reader)
			if err != nil {
				resultCh <- PipelineResult{
					ResponseIndex: i + 1,
					Err:           fmt.Errorf("pipeline read failed at response #%d: %w; first %d responses succeeded", i+1, err, i),
					SuccessBefore: i,
				}
				return
			}

			resultCh <- PipelineResult{
				ResponseIndex: i + 1,
				Reply:         reply,
				SuccessBefore: i,
			}
		}
	}()

	return resultCh, nil
}

func writeCommand(writer *bufio.Writer, args [][]byte) error {
	if len(args) == 0 {
		return fmt.Errorf("empty command args")
	}

	if _, err := writer.WriteString("*"); err != nil {
		return err
	}
	if _, err := writer.WriteString(strconv.Itoa(len(args))); err != nil {
		return err
	}
	if _, err := writer.WriteString(resp.CRLF); err != nil {
		return err
	}

	for _, arg := range args {
		if arg == nil {
			arg = []byte{}
		}
		if _, err := writer.WriteString("$"); err != nil {
			return err
		}
		if _, err := writer.WriteString(strconv.Itoa(len(arg))); err != nil {
			return err
		}
		if _, err := writer.WriteString(resp.CRLF); err != nil {
			return err
		}
		if _, err := writer.Write(arg); err != nil {
			return err
		}
		if _, err := writer.WriteString(resp.CRLF); err != nil {
			return err
		}
	}

	return nil
}

func readOneReply(reader *bufio.Reader) (_interface.Reply, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("empty resp line")
	}

	switch line[0] {
	case '+':
		return resp.MakeSimpleReply(string(line[1:])), nil
	case '-':
		return resp.MakeErrorReply(string(line[1:])), nil
	case ':':
		n, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer reply: %w", err)
		}
		return resp.MakeIntegerReply(n), nil
	case '$':
		return readBulkReply(reader, line)
	case '*':
		return readArrayReply(reader, line)
	default:
		return nil, fmt.Errorf("unknown resp type: %q", line[0])
	}
}

func readBulkReply(reader *bufio.Reader, header []byte) (_interface.Reply, error) {
	strlen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid bulk length: %w", err)
	}
	if strlen < -1 {
		return nil, fmt.Errorf("invalid bulk length: %d", strlen)
	}
	if strlen == -1 {
		return resp.MakeBulkReply(nil), nil
	}

	body := make([]byte, strlen+2)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, err
	}
	if !bytes.HasSuffix(body, []byte(resp.CRLF)) {
		return nil, fmt.Errorf("bulk reply missing CRLF")
	}
	return resp.MakeBulkReply(body[:strlen]), nil
}

func readArrayReply(reader *bufio.Reader, header []byte) (_interface.Reply, error) {
	n, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %w", err)
	}
	if n < -1 {
		return nil, fmt.Errorf("invalid array length: %d", n)
	}
	if n == -1 {
		return resp.MakeArrayReply(nil), nil
	}

	args := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		line, err := readLine(reader)
		if err != nil {
			return nil, err
		}
		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("array element #%d is not bulk string", i+1)
		}

		strlen, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid array bulk length: %w", err)
		}
		if strlen < -1 {
			return nil, fmt.Errorf("invalid array bulk length: %d", strlen)
		}

		if strlen == -1 {
			args = append(args, nil)
			continue
		}

		body := make([]byte, strlen+2)
		if _, err := io.ReadFull(reader, body); err != nil {
			return nil, err
		}
		if !bytes.HasSuffix(body, []byte(resp.CRLF)) {
			return nil, fmt.Errorf("array bulk element missing CRLF")
		}
		args = append(args, body[:strlen])
	}

	return resp.MakeArrayReply(args), nil
}

func readLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
	return line, nil
}

