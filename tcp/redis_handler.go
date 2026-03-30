package tcp

import (
	_interface "MiddlewareSelf/redis/interface"
	"MiddlewareSelf/redis/database"
	"MiddlewareSelf/redis/parser"
	"MiddlewareSelf/redis/resp"
	"MiddlewareSelf/util/atomic"
	"MiddlewareSelf/util/wait"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RedisClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

type RedisHandler struct {
	db *database.Db

	activeConn sync.Map
	closing    atomic.Boolean
}

func MakeRedisHandler(db *database.Db) *RedisHandler {
	return &RedisHandler{db: db}
}

func (h *RedisHandler) Close() error {
	h.closing.Set(true)
	h.activeConn.Range(func(key, _ interface{}) bool {
		client := key.(*RedisClient)
		_ = closeRedisClient(client)
		return true
	})

	if h.db != nil {
		h.db.Close()
	}
	return nil
}

func closeRedisClient(c *RedisClient) error {
	c.Waiting.WaitWithTimeout(30 * time.Second)
	return c.Conn.Close()
}

func (h *RedisHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
		return
	}

	client := &RedisClient{Conn: conn}
	h.activeConn.Store(client, struct{}{})
	defer func() {
		h.activeConn.Delete(client)
		if err := closeRedisClient(client); err != nil {
			log.Printf("[RedisHandler] close client error: %v", err)
		}
	}()

	cmdCh := parser.ParseStream(conn)
	currentDB := 0

	for {
		select {
		case <-ctx.Done():
			return
		case payload, ok := <-cmdCh:
			if !ok {
				return
			}
			if payload == nil {
				continue
			}
			if payload.Err != nil {
				if payload.Err == io.EOF || payload.Err.Error() == "EOF" {
					return
				}
				_ = h.writeReply(client, resp.MakeErrorReply(payload.Err.Error()))
				continue
			}

			arr, ok := payload.Data.(*resp.ArrayReply)
			if !ok {
				_ = h.writeReply(client, resp.MakeErrorReply("ERR protocol error: expected array command"))
				continue
			}
			if len(arr.Args) == 0 {
				_ = h.writeReply(client, resp.MakeErrorReply("ERR empty command"))
				continue
			}

			if strings.EqualFold(string(arr.Args[0]), "SELECT") {
				if len(arr.Args) != 2 {
					_ = h.writeReply(client, resp.MakeErrorReply("ERR wrong number of arguments for 'select'"))
					continue
				}
				nextIdx, err := strconv.Atoi(string(arr.Args[1]))
				if err != nil {
					_ = h.writeReply(client, resp.MakeErrorReply("ERR invalid index argument"))
					continue
				}
				result, err := h.db.Exec(currentDB, arr.Args)
				if err != nil {
					_ = h.writeReply(client, resp.MakeErrorReply("ERR "+err.Error()))
					continue
				}
				currentDB = nextIdx
				r := toReply(result)
				if err := h.writeReply(client, r); err != nil {
					return
				}
				continue
			}

			result, err := h.db.Exec(currentDB, arr.Args)
			if err != nil {
				_ = h.writeReply(client, resp.MakeErrorReply("ERR "+err.Error()))
				continue
			}

			reply := toReply(result)
			if err := h.writeReply(client, reply); err != nil {
				return
			}
		}
	}
}

func (h *RedisHandler) writeReply(client *RedisClient, r _interface.Reply) error {
	client.Waiting.Add(1)
	defer client.Waiting.Done()

	_ = client.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := client.Conn.Write(r.ToBytes())
	return err
}

func toReply(v interface{}) _interface.Reply {
	switch val := v.(type) {
	case nil:
		return resp.MakeBulkReply(nil)
	case string:
		return resp.MakeSimpleReply(val)
	case []byte:
		return resp.MakeBulkReply(val)
	case int:
		return resp.MakeIntegerReply(int64(val))
	case int64:
		return resp.MakeIntegerReply(val)
	default:
		return resp.MakeErrorReply(fmt.Sprintf("ERR unsupported reply type %T", v))
	}
}
