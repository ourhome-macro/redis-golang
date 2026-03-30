package database

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/datastruct"
	"MiddlewareSelf/redis/parser"
	"MiddlewareSelf/redis/resp"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

//TODO :采用函数式编程来包装exec从而引入aof

const MaxNumber = 16

type Db struct {
	//index int64
	dicts []*datastruct.Dict
	aof   *aof.AOF
}

func MakeDbs() *Db {
	dicts := make([]*datastruct.Dict, MaxNumber)
	for i := 0; i < MaxNumber; i++ {
		dicts[i] = datastruct.MakeDict()
	}
	db := &Db{
		dicts: dicts,
	}
	loadAOF(db)
	return db
}

	// 不 return 到这里


func loadAOF(db *Db) {
	path := aof.AofName
	file, err := os.Open(path)
	if err != nil {
		// 兼容旧文件名 redis.aof
		if legacyFile, legacyErr := os.Open("redis.aof"); legacyErr == nil {
			file = legacyFile
			path = "redis.aof"
		} else {
			return
		}
	}
	defer file.Close()

	ch := parser.ParseStream(file)
	log.Printf("[DB] loading AOF from %s", path)
	for payLoad := range ch {
		if payLoad == nil {
			continue
		}
		if payLoad.Err != nil {
			if payLoad.Err.Error() == "EOF" {
				break
			}
			log.Printf("[DB] skip invalid AOF payload: %v", payLoad.Err)
			continue
		}
		if arr, ok := payLoad.Data.(*resp.ArrayReply); ok {
			_, exec := db.Exec(0, arr.Args)
			if exec != nil {
				log.Printf("[DB] replay command failed: %v", exec)
			} // TODO 后续处理这个index问题
		}
	}
}

//func (db *Db) Select(index int) bool {
//	if index < 0 || index >= MaxNumber {
//		return false
//	}
//	atomic.StoreInt64(&db.index, int64(index))
//	return true
//}

func (db *Db) GetDict(index int) (*datastruct.Dict, error) {
	if index < 0 || index >= 16 {
		return nil, errors.New("index out of range")
	}
	return db.dicts[index], nil
}

// 让外层调用此函数的存储index状态
func (db *Db) Exec(index int, args [][]byte) (interface{}, error) {
	//db.mu.Lock()
	//defer db.mu.Unlock()
	if len(args) == 0 {
		return nil, errors.New("empty command")
	}

	cmd := strings.ToUpper(string(args[0]))
	if cmd == "SELECT" {
		if len(args) != 2 {
			return nil, errors.New("wrong number of arguments for 'select'")
		}
		ind, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return nil, errors.New("invalid index argument")
		}
		//index = ind
		if ind >= 0 && ind < 16 {
			return "OK", nil
		}
		return nil, errors.New("DB index out of range")
	}

	dict, err := db.GetDict(index)
	if err != nil {
		return nil, err
	}
	var reply interface{}

	switch cmd {
	case "SET":
		if len(args) != 3 {
			return nil, errors.New("wrong number of arguments for 'set'")
		}
		key := string(args[1])
		val := NewDataObject(args[2])
		dict.Set(key, val)
		reply = "OK" // Redis SET 返回 OK

	case "SETWITHTTL":
		if len(args) != 4 {
			return nil, errors.New("wrong number of arguments for 'setwithttl'")
		}
		key := string(args[1])
		val := NewDataObject(args[2])
		ttl, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			return nil, errors.New("invalid ttl argument")
		}
		dict.SetWithTTL(key, val, ttl)
		reply = "OK"

	case "GET":
		if len(args) != 2 {
			return nil, errors.New("wrong number of arguments for 'get'")
		}
		key := string(args[1])
		val, ok := dict.Get(key)
		if !ok {
			return nil, nil
		}
		if dobj, ok := val.(*DataObject); ok {
			reply = dobj.Bytes()
		}

	case "DEL":
		if len(args) < 2 {
			return nil, errors.New("wrong number of arguments for 'del'")
		}
		count := 0
		for i := 1; i < len(args); i++ {
			key := string(args[i])
			if _, ok := dict.Get(key); ok {
				dict.Remove(key)
				count++
			}
		}
		reply = count
	default:
		return nil, fmt.Errorf("unknown command '%s'", cmd)
	}

	if aof.IsWriteCmd(cmd) {
		if db.aof != nil {
			if err := db.aof.AppendCommand(args); err != nil {
				return nil, err
			}
		}
	}

	return reply, nil
}

// EnableAOF 启用 AOF 持久化并注册 rewrite 快照回调。
func (db *Db) EnableAOF(policy aof.SyncPolicy) error {
	if db.aof != nil {
		return nil
	}
	a, err := aof.NewAOF(policy)
	if err != nil {
		return err
	}
	a.SetSnapshotProvider(db.snapshotForRewrite)
	db.aof = a
	return nil
}

// RewriteAOF 触发一次后台重写流程。
func (db *Db) RewriteAOF(ctx context.Context) error {
	if db.aof == nil {
		return errors.New("aof is not enabled")
	}
	return db.aof.Rewrite(ctx)
}

// StartAutoRewriteLoop 启动 AOF 自动重写后台循环。
func (db *Db) StartAutoRewriteLoop(interval time.Duration, minSizeBytes int64, growthPercent float64) error {
	if db.aof == nil {
		return errors.New("aof is not enabled")
	}
	db.aof.StartAutoRewriteLoop(interval, minSizeBytes, growthPercent)
	return nil
}

// Close 关闭底层资源（当前主要是 AOF 文件与后台协程）。
func (db *Db) Close() {
	if db.aof != nil {
		db.aof.Close()
	}
}

func (db *Db) snapshotForRewrite() ([]aof.RewriteCommand, error) {
	now := int64(0)
	now = time.Now().UnixNano()

	commands := make([]aof.RewriteCommand, 0)
	for _, dict := range db.dicts {
		items := dict.Snapshot()
		for _, item := range items {
			bytesGetter, ok := item.Value.(interface{ Bytes() []byte })
			if !ok {
				continue
			}
			val := bytesGetter.Bytes()
			valCopy := make([]byte, len(val))
			copy(valCopy, val)

			if item.ExpireAtNano > 0 {
				ttlMs := (item.ExpireAtNano - now) / 1e6
				if ttlMs <= 0 {
					continue
				}
				commands = append(commands, aof.RewriteCommand{Args: [][]byte{
					[]byte("SETWITHTTL"),
					[]byte(item.Key),
					valCopy,
					[]byte(strconv.FormatInt(ttlMs, 10)),
				}})
			} else {
				commands = append(commands, aof.RewriteCommand{Args: [][]byte{
					[]byte("SET"),
					[]byte(item.Key),
					valCopy,
				}})
			}
		}
	}

	return commands, nil
}
