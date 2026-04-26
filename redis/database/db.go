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
	"sync"
	"sync/atomic"
	"time"
)

const MaxNumber = 16
const maxInt64 = int64(1<<63 - 1)

type Db struct {
	dicts        []*datastruct.Dict
	aof          *aof.AOF
	mu           sync.Mutex
	dirty        int64
	lastSaveUnix int64
}

type commandPlan struct {
	write       bool
	aofCommands [][][]byte
	exec        func() (interface{}, error)
}

func MakeDbs() *Db {
	dicts := make([]*datastruct.Dict, MaxNumber)
	for i := 0; i < MaxNumber; i++ {
		dicts[i] = datastruct.MakeDict()
	}

	db := &Db{dicts: dicts}
	loadAOF(db)
	atomic.StoreInt64(&db.lastSaveUnix, time.Now().Unix())
	return db
}

func loadAOF(db *Db) {
	path := aof.AofName
	file, err := os.Open(path)
	if err != nil {
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

	currentDB := 0
	for payload := range ch {
		if payload == nil {
			continue
		}
		if payload.Err != nil {
			if payload.Err.Error() == "EOF" {
				break
			}
			log.Printf("[DB] skip invalid AOF payload: %v", payload.Err)
			continue
		}

		arr, ok := payload.Data.(*resp.ArrayReply)
		if !ok {
			continue
		}

		if _, err := db.Exec(currentDB, arr.Args); err != nil {
			log.Printf("[DB] replay command failed: %v", err)
			continue
		}

		if nextDB, ok := parseSelectIndex(arr.Args); ok {
			currentDB = nextDB
		}
	}
}

func (db *Db) GetDict(index int) (*datastruct.Dict, error) {
	if index < 0 || index >= MaxNumber {
		return nil, errors.New("index out of range")
	}
	return db.dicts[index], nil
}

func (db *Db) Exec(index int, args [][]byte) (interface{}, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	plan, err := db.makeCommandPlan(index, args)
	if err != nil {
		return nil, err
	}

	if plan.write && db.aof != nil {
		if err := db.aof.AppendCommands(index, plan.aofCommands); err != nil {
			return nil, err
		}
	}

	reply, err := plan.exec()
	if err != nil {
		return nil, err
	}
	if plan.write {
		atomic.AddInt64(&db.dirty, 1)
	}

	return reply, nil
}

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

func (db *Db) RewriteAOF(ctx context.Context) error {
	if db.aof == nil {
		return errors.New("aof is not enabled")
	}
	return db.aof.Rewrite(ctx)
}

func (db *Db) StartAutoRewriteLoop(interval time.Duration, minSizeBytes int64, growthPercent float64) error {
	if db.aof == nil {
		return errors.New("aof is not enabled")
	}
	db.aof.StartAutoRewriteLoop(interval, minSizeBytes, growthPercent)
	return nil
}

func (db *Db) Close() {
	if db.aof != nil {
		db.aof.Close()
	}
}

func (db *Db) snapshotForRewrite() ([]aof.RewriteCommand, error) {
	now := time.Now().UnixNano()
	commands := make([]aof.RewriteCommand, 0)

	for dbIndex, dict := range db.dicts {
		items := dict.Snapshot()
		if len(items) == 0 {
			continue
		}

		dbCommands := make([]aof.RewriteCommand, 0, len(items))
		for _, item := range items {
			bytesGetter, ok := item.Value.(interface{ Bytes() []byte })
			if !ok {
				continue
			}

			val := bytesGetter.Bytes()
			valCopy := make([]byte, len(val))
			copy(valCopy, val)

			if item.ExpireAtNano > 0 {
				if item.ExpireAtNano <= now {
					continue
				}
				dbCommands = append(dbCommands, aof.RewriteCommand{Args: makeCommandBytes(
					[]byte("SETWITHPXAT"),
					[]byte(item.Key),
					valCopy,
					[]byte(strconv.FormatInt(item.ExpireAtNano/1e6, 10)),
				)})
				continue
			}

			dbCommands = append(dbCommands, aof.RewriteCommand{Args: makeCommandBytes(
				[]byte("SET"),
				[]byte(item.Key),
				valCopy,
			)})
		}
		if len(dbCommands) == 0 {
			continue
		}

		commands = append(commands, aof.RewriteCommand{Args: makeCommand("SELECT", strconv.Itoa(dbIndex))})
		commands = append(commands, dbCommands...)
	}

	return commands, nil
}

func (db *Db) makeCommandPlan(index int, args [][]byte) (commandPlan, error) {
	if len(args) == 0 {
		return commandPlan{}, errors.New("empty command")
	}

	cmd := strings.ToUpper(string(args[0]))
	if cmd == "INFO" {
		if len(args) > 2 {
			return commandPlan{}, errors.New("wrong number of arguments for 'info'")
		}
		section := "default"
		if len(args) == 2 {
			section = strings.ToLower(string(args[1]))
		}
		return commandPlan{
			exec: func() (interface{}, error) {
				return db.infoReply(section)
			},
		}, nil
	}
	if cmd == "SELECT" {
		if len(args) != 2 {
			return commandPlan{}, errors.New("wrong number of arguments for 'select'")
		}

		nextDB, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return commandPlan{}, errors.New("invalid index argument")
		}
		if nextDB < 0 || nextDB >= MaxNumber {
			return commandPlan{}, errors.New("DB index out of range")
		}

		return commandPlan{
			exec: func() (interface{}, error) {
				return "OK", nil
			},
		}, nil
	}

	dict, err := db.GetDict(index)
	if err != nil {
		return commandPlan{}, err
	}

	switch cmd {
	case "SET":
		if len(args) != 3 {
			return commandPlan{}, errors.New("wrong number of arguments for 'set'")
		}

		key := string(args[1])
		value := append([]byte(nil), args[2]...)
		aofCommands := db.setAOFCommands(dict, key, value, 0)
		return commandPlan{
			write:       true,
			aofCommands: aofCommands,
			exec: func() (interface{}, error) {
				dict.Set(key, NewDataObject(value))
				return "OK", nil
			},
		}, nil

	case "SETWITHTTL":
		if len(args) != 4 {
			return commandPlan{}, errors.New("wrong number of arguments for 'setwithttl'")
		}

		key := string(args[1])
		value := append([]byte(nil), args[2]...)
		ttl, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			return commandPlan{}, errors.New("invalid ttl argument")
		}
		if ttl <= 0 {
			return commandPlan{}, errors.New("invalid ttl argument")
		}
		expireAtMs, expireAtNano, err := expireAtFromTTL(ttl)
		if err != nil {
			return commandPlan{}, errors.New("invalid ttl argument")
		}

		return commandPlan{
			write:       true,
			aofCommands: db.setAOFCommands(dict, key, value, expireAtMs),
			exec: func() (interface{}, error) {
				dict.SetWithExpireAt(key, NewDataObject(value), expireAtNano)
				return "OK", nil
			},
		}, nil

	case "SETWITHPXAT":
		if len(args) != 4 {
			return commandPlan{}, errors.New("wrong number of arguments for 'setwithpxat'")
		}

		key := string(args[1])
		value := append([]byte(nil), args[2]...)
		expireAtMs, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			return commandPlan{}, errors.New("invalid expire time")
		}
		expireAtNano, err := expireAtNanoFromMs(expireAtMs)
		if err != nil {
			return commandPlan{}, errors.New("invalid expire time")
		}

		return commandPlan{
			write:       true,
			aofCommands: db.setAOFCommands(dict, key, value, expireAtMs),
			exec: func() (interface{}, error) {
				dict.SetWithExpireAt(key, NewDataObject(value), expireAtNano)
				return "OK", nil
			},
		}, nil

	case "EXPIRE":
		if len(args) != 3 {
			return commandPlan{}, errors.New("wrong number of arguments for 'expire'")
		}

		key := string(args[1])
		ttlSec, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return commandPlan{}, errors.New("invalid expire time")
		}
		ttlMs := int64(0)
		expireAtMs := int64(0)
		expireAtNano := int64(0)
		if ttlSec > 0 {
			if ttlSec > maxInt64/1000 {
				return commandPlan{}, errors.New("invalid expire time")
			}
			ttlMs = ttlSec * 1000
			expireAtMs, expireAtNano, err = expireAtFromTTL(ttlMs)
			if err != nil {
				return commandPlan{}, errors.New("invalid expire time")
			}
		}

		return commandPlan{
			write:       true,
			aofCommands: expireAOFCommands(key, ttlMs, expireAtMs),
			exec: func() (interface{}, error) {
				if ttlMs <= 0 {
					if dict.ExpireAt(key, time.Now().UnixNano()) {
						return int64(1), nil
					}
					return int64(0), nil
				}
				if dict.ExpireAt(key, expireAtNano) {
					return int64(1), nil
				}
				return int64(0), nil
			},
		}, nil

	case "PEXPIRE":
		if len(args) != 3 {
			return commandPlan{}, errors.New("wrong number of arguments for 'pexpire'")
		}

		key := string(args[1])
		ttlMs, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return commandPlan{}, errors.New("invalid expire time")
		}
		expireAtMs := int64(0)
		expireAtNano := int64(0)
		if ttlMs > 0 {
			expireAtMs, expireAtNano, err = expireAtFromTTL(ttlMs)
			if err != nil {
				return commandPlan{}, errors.New("invalid expire time")
			}
		}

		return commandPlan{
			write:       true,
			aofCommands: expireAOFCommands(key, ttlMs, expireAtMs),
			exec: func() (interface{}, error) {
				if ttlMs <= 0 {
					if dict.ExpireAt(key, time.Now().UnixNano()) {
						return int64(1), nil
					}
					return int64(0), nil
				}
				if dict.ExpireAt(key, expireAtNano) {
					return int64(1), nil
				}
				return int64(0), nil
			},
		}, nil

	case "PEXPIREAT":
		if len(args) != 3 {
			return commandPlan{}, errors.New("wrong number of arguments for 'pexpireat'")
		}

		key := string(args[1])
		expireAtMs, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return commandPlan{}, errors.New("invalid expire time")
		}
		expireAtNano, err := expireAtNanoFromMs(expireAtMs)
		if err != nil {
			return commandPlan{}, errors.New("invalid expire time")
		}

		return commandPlan{
			write:       true,
			aofCommands: [][][]byte{makeCommand("PEXPIREAT", key, strconv.FormatInt(expireAtMs, 10))},
			exec: func() (interface{}, error) {
				if dict.ExpireAt(key, expireAtNano) {
					return int64(1), nil
				}
				return int64(0), nil
			},
		}, nil

	case "TTL":
		if len(args) != 2 {
			return commandPlan{}, errors.New("wrong number of arguments for 'ttl'")
		}

		key := string(args[1])
		return commandPlan{
			exec: func() (interface{}, error) {
				return dict.TTL(key), nil
			},
		}, nil

	case "PTTL":
		if len(args) != 2 {
			return commandPlan{}, errors.New("wrong number of arguments for 'pttl'")
		}

		key := string(args[1])
		return commandPlan{
			exec: func() (interface{}, error) {
				return dict.PTTL(key), nil
			},
		}, nil

	case "PERSIST":
		if len(args) != 2 {
			return commandPlan{}, errors.New("wrong number of arguments for 'persist'")
		}

		key := string(args[1])
		return commandPlan{
			write:       true,
			aofCommands: [][][]byte{makeCommand("PERSIST", key)},
			exec: func() (interface{}, error) {
				if dict.Persist(key) {
					return int64(1), nil
				}
				return int64(0), nil
			},
		}, nil

	case "GET":
		if len(args) != 2 {
			return commandPlan{}, errors.New("wrong number of arguments for 'get'")
		}

		key := string(args[1])
		return commandPlan{
			exec: func() (interface{}, error) {
				val, ok := dict.Get(key)
				if !ok {
					return nil, nil
				}

				dobj, ok := val.(*DataObject)
				if !ok {
					return nil, fmt.Errorf("unexpected value type %T", val)
				}
				return dobj.Bytes(), nil
			},
		}, nil

	case "DEL":
		if len(args) < 2 {
			return commandPlan{}, errors.New("wrong number of arguments for 'del'")
		}

		keys := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys = append(keys, string(args[i]))
		}

		return commandPlan{
			write:       true,
			aofCommands: [][][]byte{makeCommand(append([]string{"DEL"}, keys...)...)},
			exec: func() (interface{}, error) {
				count := 0
				for _, key := range keys {
					if _, ok := dict.Get(key); ok {
						dict.Remove(key)
						count++
					}
				}
				return count, nil
			},
		}, nil
	default:
		return commandPlan{}, fmt.Errorf("unknown command '%s'", cmd)
	}
}

func (db *Db) setAOFCommands(dict *datastruct.Dict, key string, value []byte, expireAtMs int64) [][][]byte {
	var primary [][]byte
	if expireAtMs > 0 {
		primary = makeCommandBytes(
			[]byte("SETWITHPXAT"),
			[]byte(key),
			value,
			[]byte(strconv.FormatInt(expireAtMs, 10)),
		)
	} else {
		primary = makeCommandBytes([]byte("SET"), []byte(key), value)
	}

	commands := [][][]byte{primary}
	if expireAtMs > 0 && expireAtMs <= time.Now().UnixMilli() {
		return commands
	}

	evicted := dict.PreviewSetEvictions(key, len(value))
	if len(evicted) == 0 {
		return commands
	}

	delArgs := make([]string, 0, len(evicted)+1)
	delArgs = append(delArgs, "DEL")
	delArgs = append(delArgs, evicted...)
	return append(commands, makeCommand(delArgs...))
}

func expireAOFCommands(key string, ttlMs int64, expireAtMs int64) [][][]byte {
	if ttlMs <= 0 {
		return [][][]byte{makeCommand("DEL", key)}
	}
	return [][][]byte{makeCommand("PEXPIREAT", key, strconv.FormatInt(expireAtMs, 10))}
}

func expireAtFromTTL(ttlMs int64) (expireAtMs int64, expireAtNano int64, err error) {
	nowMs := time.Now().UnixMilli()
	if ttlMs > maxInt64-nowMs {
		return 0, 0, errors.New("expire time overflow")
	}
	expireAtMs = nowMs + ttlMs
	expireAtNano, err = expireAtNanoFromMs(expireAtMs)
	return expireAtMs, expireAtNano, err
}

func expireAtNanoFromMs(expireAtMs int64) (int64, error) {
	if expireAtMs <= time.Now().UnixMilli() {
		return time.Now().UnixNano(), nil
	}
	if expireAtMs > maxInt64/1e6 {
		return 0, errors.New("expire time overflow")
	}
	return expireAtMs * 1e6, nil
}

func makeCommand(args ...string) [][]byte {
	out := make([][]byte, 0, len(args))
	for _, arg := range args {
		out = append(out, []byte(arg))
	}
	return out
}

func makeCommandBytes(args ...[]byte) [][]byte {
	out := make([][]byte, 0, len(args))
	for _, arg := range args {
		argCopy := make([]byte, len(arg))
		copy(argCopy, arg)
		out = append(out, argCopy)
	}
	return out
}

func parseSelectIndex(args [][]byte) (int, bool) {
	if len(args) != 2 {
		return 0, false
	}
	if !strings.EqualFold(string(args[0]), "SELECT") {
		return 0, false
	}

	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return 0, false
	}
	return index, true
}

func (db *Db) infoReply(section string) ([]byte, error) {
	switch section {
	case "default", "all", "persistence":
		return db.persistenceInfoBytes(), nil
	default:
		return nil, fmt.Errorf("unsupported INFO section '%s'", section)
	}
}

func (db *Db) persistenceInfoBytes() []byte {
	var info aof.PersistenceInfo
	if db.aof != nil {
		info = db.aof.PersistenceInfo()
	}

	lines := []string{
		"# Persistence",
		fmt.Sprintf("loading:%d", boolToInt(info.Loading)),
		fmt.Sprintf("rdb_changes_since_last_save:%d", atomic.LoadInt64(&db.dirty)),
		fmt.Sprintf("rdb_last_save_time:%d", atomic.LoadInt64(&db.lastSaveUnix)),
		fmt.Sprintf("aof_enabled:%d", boolToInt(info.AOFEnabled)),
		fmt.Sprintf("aof_rewrite_in_progress:%d", boolToInt(info.AOFRewriteInProgress)),
		fmt.Sprintf("aof_rewrite_scheduled:%d", boolToInt(info.AOFRewriteScheduled)),
		fmt.Sprintf("aof_last_rewrite_time_sec:%d", info.AOFLastRewriteSec),
		fmt.Sprintf("aof_current_rewrite_time_sec:%d", info.AOFCurrentRewriteSec),
		fmt.Sprintf("aof_last_bgrewrite_status:%s", statusString(info.AOFLastBGRewriteOK)),
		fmt.Sprintf("aof_last_write_status:%s", statusString(info.AOFLastWriteOK)),
		fmt.Sprintf("aof_current_size:%d", info.AOFCurrentSize),
		fmt.Sprintf("aof_base_size:%d", info.AOFBaseSize),
		fmt.Sprintf("aof_buffer_length:%d", info.AOFBufferLength),
		fmt.Sprintf("aof_rewrite_count:%d", info.AOFRewriteCount),
	}

	return []byte(strings.Join(lines, "\r\n") + "\r\n")
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func statusString(ok bool) string {
	if ok {
		return "ok"
	}
	return "err"
}
