package tcp

import (
	"MiddlewareSelf/redis/database"
	_interface "MiddlewareSelf/redis/interface"
	"MiddlewareSelf/redis/parser"
	"MiddlewareSelf/redis/resp"
	"MiddlewareSelf/util/atomic"
	"MiddlewareSelf/util/wait"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RedisClient struct {
	Conn             net.Conn
	Waiting          wait.Wait
	ReplicaSession   *database.ReplicaSession
	ReplicaPort      int
	ReplicaCapablist []string
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
		if client.ReplicaSession != nil {
			client.ReplicaSession.Close()
		}
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
				if errors.Is(payload.Err, io.EOF) || errors.Is(payload.Err, os.ErrDeadlineExceeded) {
					return
				}
				_ = h.writeError(client, payload.Err)
				continue
			}

			arr, ok := payload.Data.(*resp.ArrayReply)
			if !ok {
				_ = h.writeErrorMessage(client, "protocol error: expected array command")
				continue
			}
			if len(arr.Args) == 0 {
				_ = h.writeErrorMessage(client, "empty command")
				continue
			}
			if h.handleReplicationCommand(client, arr.Args) {
				continue
			}

			selectingDB := strings.EqualFold(string(arr.Args[0]), "SELECT")
			result, err := h.db.Exec(currentDB, arr.Args)
			if err != nil {
				_ = h.writeError(client, err)
				continue
			}
			if selectingDB {
				currentDB, _ = strconv.Atoi(string(arr.Args[1]))
			}

			reply := toReply(result)
			if err := h.writeReply(client, reply); err != nil {
				return
			}
		}
	}
}

func (h *RedisHandler) writeReply(client *RedisClient, r _interface.Reply) error {
	return h.writeBytes(client, r.ToBytes())
}

func (h *RedisHandler) writeBytes(client *RedisClient, payload []byte) error {
	client.Waiting.Add(1)
	defer client.Waiting.Done()

	_ = client.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := client.Conn.Write(payload)
	return err
}

func (h *RedisHandler) writeError(client *RedisClient, err error) error {
	return h.writeErrorMessage(client, err.Error())
}

func (h *RedisHandler) writeErrorMessage(client *RedisClient, message string) error {
	return h.writeReply(client, resp.MakeStandardErrorReply(message))
}

func (h *RedisHandler) handleReplicationCommand(client *RedisClient, args [][]byte) bool {
	if len(args) == 0 {
		return false
	}

	switch strings.ToUpper(string(args[0])) {
	case "REPLCONF":
		h.handleReplConf(client, args)
		return true
	case "PSYNC":
		h.handlePSync(client, args)
		return true
	default:
		return false
	}
}

func (h *RedisHandler) handleReplConf(client *RedisClient, args [][]byte) {
	if len(args) < 3 || len(args)%2 == 0 {
		_ = h.writeErrorMessage(client, "wrong number of arguments for 'replconf'")
		return
	}

	if len(args) == 3 && strings.EqualFold(string(args[1]), "ack") {
		offset, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			_ = h.writeErrorMessage(client, "value is not an integer or out of range")
			return
		}
		if client.ReplicaSession != nil {
			client.ReplicaSession.Ack(offset)
		}
		return
	}

	for i := 1; i < len(args); i += 2 {
		key := strings.ToLower(string(args[i]))
		value := string(args[i+1])
		switch key {
		case "listening-port":
			port, err := strconv.Atoi(value)
			if err != nil || port < 0 {
				_ = h.writeErrorMessage(client, "value is not an integer or out of range")
				return
			}
			client.ReplicaPort = port
		case "capa":
			client.ReplicaCapablist = append(client.ReplicaCapablist, value)
		}
	}

	_ = h.writeReply(client, resp.MakeSimpleReply("OK"))
}

func (h *RedisHandler) handlePSync(client *RedisClient, args [][]byte) {
	if len(args) != 3 {
		_ = h.writeErrorMessage(client, "wrong number of arguments for 'psync'")
		return
	}

	offset, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		_ = h.writeErrorMessage(client, "value is not an integer or out of range")
		return
	}
	if client.ReplicaSession != nil {
		client.ReplicaSession.Close()
		client.ReplicaSession = nil
	}

	meta := database.ReplicaMetadata{
		ListeningPort: client.ReplicaPort,
		Capabilities:  append([]string(nil), client.ReplicaCapablist...),
		RemoteAddr:    client.Conn.RemoteAddr().String(),
	}

	requestedID := string(args[1])
	if requestedID != "?" && offset >= 0 {
		if session, backlog, ok := h.db.TryPartialResync(client.Conn, meta, requestedID, offset); ok {
			client.ReplicaSession = session
			if err := h.writeBytes(client, resp.MakeSimpleReply("CONTINUE").ToBytes()); err != nil {
				session.Close()
				client.ReplicaSession = nil
				return
			}
			if len(backlog) > 0 {
				if err := h.writeBytes(client, backlog); err != nil {
					session.Close()
					client.ReplicaSession = nil
					return
				}
			}
			if err := session.Activate(); err != nil {
				session.Close()
				client.ReplicaSession = nil
			}
			return
		}
	}

	session, replID, replOffset, snapshot, err := h.db.BeginFullResync(client.Conn, meta)
	if err != nil {
		_ = h.writeError(client, err)
		return
	}
	client.ReplicaSession = session

	header := resp.MakeSimpleReply(fmt.Sprintf("FULLRESYNC %s %d", replID, replOffset)).ToBytes()
	if err := h.writeBytes(client, header); err != nil {
		session.Close()
		client.ReplicaSession = nil
		return
	}
	if err := h.writeReply(client, resp.MakeBulkReply(snapshot)); err != nil {
		session.Close()
		client.ReplicaSession = nil
		return
	}
	if err := session.Activate(); err != nil {
		session.Close()
		client.ReplicaSession = nil
	}
}

func toReply(v interface{}) _interface.Reply {
	switch val := v.(type) {
	case _interface.Reply:
		return val
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
		return resp.MakeStandardErrorReply(fmt.Sprintf("unsupported reply type %T", v))
	}
}
