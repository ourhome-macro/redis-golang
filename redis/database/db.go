package database

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/datastruct"
	"MiddlewareSelf/redis/parser"
	"MiddlewareSelf/redis/resp"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const MaxNumber = 16
const maxInt64 = int64(1<<63 - 1)

var (
	errAOFDisabled           = errors.New("AOF is not enabled")
	errDBIndexOutOfRange     = errors.New("DB index is out of range")
	errValueOutOfRange       = errors.New("value is not an integer or out of range")
	errUnsupportedInfoFormat = "unsupported INFO section '%s'"
)

type Db struct {
	dicts                  []*datastruct.Dict
	aof                    *aof.AOF
	replication            *replicationState
	mu                     sync.Mutex
	startTime              time.Time
	activeExpireStop       chan struct{}
	activeExpireWG         sync.WaitGroup
	closeActiveExpireOnce  sync.Once
	dirty                  int64
	lastSaveUnix           int64
	totalCommands          int64
	activeExpiredKeys      int64
	activeExpireCycles     int64
	activeExpireRunning    int32
	activeExpireIntervalMs int64
	activeExpireLimitPerDB int64
}

type commandPlan struct {
	write       bool
	aofCommands [][][]byte
	exec        func() (interface{}, error)
}

func MakeDbs() *Db {
	db, err := OpenDbs()
	if err != nil {
		panic(err)
	}
	return db
}

func OpenDbs() (*Db, error) {
	dicts := make([]*datastruct.Dict, MaxNumber)
	for i := 0; i < MaxNumber; i++ {
		dicts[i] = datastruct.MakeDict()
	}

	now := time.Now()
	db := &Db{
		dicts:            dicts,
		replication:      newReplicationState(defaultReplicationBacklogSize),
		startTime:        now,
		activeExpireStop: make(chan struct{}),
	}
	if err := loadAOF(db); err != nil {
		return nil, err
	}
	atomic.StoreInt64(&db.lastSaveUnix, now.Unix())
	return db, nil
}

func loadAOF(db *Db) error {
	path := aof.AofName
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open AOF %s failed: %w", path, err)
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
			if errors.Is(payload.Err, io.EOF) {
				break
			}
			return fmt.Errorf("invalid AOF payload in %s: %w", path, payload.Err)
		}

		arr, ok := payload.Data.(*resp.ArrayReply)
		if !ok {
			return fmt.Errorf("invalid AOF entry in %s: expected array command, got %T", path, payload.Data)
		}

		if _, err := db.exec(currentDB, arr.Args, false); err != nil {
			return fmt.Errorf("replay AOF command %q on db %d failed: %w", commandName(arr.Args), currentDB, err)
		}

		if nextDB, ok := parseSelectIndex(arr.Args); ok {
			currentDB = nextDB
		}
	}
	return nil
}

func (db *Db) GetDict(index int) (*datastruct.Dict, error) {
	if index < 0 || index >= MaxNumber {
		return nil, errDBIndexOutOfRange
	}
	return db.dicts[index], nil
}

func (db *Db) Exec(index int, args [][]byte) (interface{}, error) {
	return db.exec(index, args, true)
}

func (db *Db) exec(index int, args [][]byte, recordStats bool) (interface{}, error) {
	db.mu.Lock()
	plan, err := db.makeCommandPlan(index, args)
	if err != nil {
		db.mu.Unlock()
		return nil, err
	}

	if plan.write && db.aof != nil {
		if err := db.aof.AppendCommands(index, plan.aofCommands); err != nil {
			db.mu.Unlock()
			return nil, err
		}
	}

	reply, err := plan.exec()
	if err != nil {
		db.mu.Unlock()
		return nil, err
	}
	if recordStats && plan.write {
		atomic.AddInt64(&db.dirty, 1)
	}
	if recordStats {
		atomic.AddInt64(&db.totalCommands, 1)
	}
	if plan.write && len(plan.aofCommands) > 0 && db.replication != nil {
		db.replication.Propagate(index, plan.aofCommands)
	}
	db.mu.Unlock()

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
		return errAOFDisabled
	}
	return db.aof.Rewrite(ctx)
}

func (db *Db) StartAutoRewriteLoop(interval time.Duration, minSizeBytes int64, growthPercent float64) error {
	if db.aof == nil {
		return errAOFDisabled
	}
	db.aof.StartAutoRewriteLoop(interval, minSizeBytes, growthPercent)
	return nil
}

func (db *Db) StartActiveExpireLoop(interval time.Duration, limitPerDB int) error {
	if interval <= 0 {
		return errors.New("active expire interval must be positive")
	}
	if limitPerDB < 0 {
		return errors.New("active expire limit must not be negative")
	}
	if !atomic.CompareAndSwapInt32(&db.activeExpireRunning, 0, 1) {
		return nil
	}

	atomic.StoreInt64(&db.activeExpireIntervalMs, interval.Milliseconds())
	atomic.StoreInt64(&db.activeExpireLimitPerDB, int64(limitPerDB))

	db.activeExpireWG.Add(1)
	go func() {
		defer db.activeExpireWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-db.activeExpireStop:
				return
			case <-ticker.C:
				db.runActiveExpireCycle(limitPerDB)
			}
		}
	}()

	return nil
}

func (db *Db) runActiveExpireCycle(limitPerDB int) int {
	expired := 0
	for _, dict := range db.dicts {
		expired += dict.RemoveExpired(limitPerDB)
	}
	atomic.AddInt64(&db.activeExpireCycles, 1)
	if expired > 0 {
		atomic.AddInt64(&db.activeExpiredKeys, int64(expired))
	}
	return expired
}

func (db *Db) Close() {
	db.closeActiveExpireOnce.Do(func() {
		if db.activeExpireStop != nil {
			close(db.activeExpireStop)
		}
		db.activeExpireWG.Wait()
		atomic.StoreInt32(&db.activeExpireRunning, 0)
	})
	if db.aof != nil {
		db.aof.Close()
	}
	if db.replication != nil {
		db.replication.Close()
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
	spec, err := commandSpecForArgs(args)
	if err != nil {
		return commandPlan{}, err
	}

	cmd := spec.name
	if cmd == "PING" {
		if len(args) == 2 {
			message := append([]byte(nil), args[1]...)
			return commandPlan{
				exec: func() (interface{}, error) {
					return message, nil
				},
			}, nil
		}
		return commandPlan{
			exec: func() (interface{}, error) {
				return "PONG", nil
			},
		}, nil
	}
	if cmd == "ECHO" {
		message := append([]byte(nil), args[1]...)
		return commandPlan{
			exec: func() (interface{}, error) {
				return message, nil
			},
		}, nil
	}
	if cmd == "INFO" {
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
		nextDB, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return commandPlan{}, errValueOutOfRange
		}
		if nextDB < 0 || nextDB >= MaxNumber {
			return commandPlan{}, errDBIndexOutOfRange
		}

		return commandPlan{
			exec: func() (interface{}, error) {
				return "OK", nil
			},
		}, nil
	}
	if cmd == "BGREWRITEAOF" {
		return commandPlan{
			exec: func() (interface{}, error) {
				if db.aof == nil {
					return nil, errAOFDisabled
				}
				if err := db.aof.RewriteAsync(30 * time.Second); err != nil {
					return nil, err
				}
				return "Background append only file rewriting started", nil
			},
		}, nil
	}

	dict, err := db.GetDict(index)
	if err != nil {
		return commandPlan{}, err
	}

	switch cmd {
	case "EXISTS":
		keys := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys = append(keys, string(args[i]))
		}
		return commandPlan{
			exec: func() (interface{}, error) {
				var count int64
				for _, key := range keys {
					if _, ok := dict.Get(key); ok {
						count++
					}
				}
				return count, nil
			},
		}, nil

	case "MGET":
		keys := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys = append(keys, string(args[i]))
		}
		return commandPlan{
			exec: func() (interface{}, error) {
				values := make([][]byte, 0, len(keys))
				for _, key := range keys {
					val, ok := dict.Get(key)
					if !ok {
						values = append(values, nil)
						continue
					}
					dobj, ok := val.(*DataObject)
					if !ok {
						return nil, fmt.Errorf("unexpected value type %T", val)
					}
					values = append(values, append([]byte(nil), dobj.Bytes()...))
				}
				return resp.MakeArrayReply(values), nil
			},
		}, nil

	case "SET":
		key := string(args[1])
		value := append([]byte(nil), args[2]...)
		aofCommands := db.setAOFCommands(dict, key, value, 0)
		return commandPlan{
			write:       spec.write,
			aofCommands: aofCommands,
			exec: func() (interface{}, error) {
				dict.Set(key, NewDataObject(value))
				return "OK", nil
			},
		}, nil

	case "MSET":
		pairs := make([][]byte, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			pairs = append(pairs, append([]byte(nil), args[i]...))
		}
		aofArgs := copyCommand(args)
		return commandPlan{
			write:       spec.write,
			aofCommands: [][][]byte{aofArgs},
			exec: func() (interface{}, error) {
				for i := 0; i < len(pairs); i += 2 {
					key := string(pairs[i])
					value := append([]byte(nil), pairs[i+1]...)
					dict.Set(key, NewDataObject(value))
				}
				return "OK", nil
			},
		}, nil

	case "SETWITHTTL":
		key := string(args[1])
		value := append([]byte(nil), args[2]...)
		ttl, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			return commandPlan{}, errValueOutOfRange
		}
		if ttl <= 0 {
			return commandPlan{}, errValueOutOfRange
		}
		expireAtMs, expireAtNano, err := expireAtFromTTL(ttl)
		if err != nil {
			return commandPlan{}, errValueOutOfRange
		}

		return commandPlan{
			write:       spec.write,
			aofCommands: db.setAOFCommands(dict, key, value, expireAtMs),
			exec: func() (interface{}, error) {
				dict.SetWithExpireAt(key, NewDataObject(value), expireAtNano)
				return "OK", nil
			},
		}, nil

	case "SETWITHPXAT":
		key := string(args[1])
		value := append([]byte(nil), args[2]...)
		expireAtMs, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			return commandPlan{}, errValueOutOfRange
		}
		expireAtNano, err := expireAtNanoFromMs(expireAtMs)
		if err != nil {
			return commandPlan{}, errValueOutOfRange
		}

		return commandPlan{
			write:       spec.write,
			aofCommands: db.setAOFCommands(dict, key, value, expireAtMs),
			exec: func() (interface{}, error) {
				dict.SetWithExpireAt(key, NewDataObject(value), expireAtNano)
				return "OK", nil
			},
		}, nil

	case "EXPIRE":
		key := string(args[1])
		ttlSec, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return commandPlan{}, errValueOutOfRange
		}
		ttlMs := int64(0)
		expireAtMs := int64(0)
		expireAtNano := int64(0)
		if ttlSec > 0 {
			if ttlSec > maxInt64/1000 {
				return commandPlan{}, errValueOutOfRange
			}
			ttlMs = ttlSec * 1000
			expireAtMs, expireAtNano, err = expireAtFromTTL(ttlMs)
			if err != nil {
				return commandPlan{}, errValueOutOfRange
			}
		}
		if !dict.Exists(key) {
			return staticIntegerPlan(0), nil
		}

		return commandPlan{
			write:       spec.write,
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
		key := string(args[1])
		ttlMs, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return commandPlan{}, errValueOutOfRange
		}
		expireAtMs := int64(0)
		expireAtNano := int64(0)
		if ttlMs > 0 {
			expireAtMs, expireAtNano, err = expireAtFromTTL(ttlMs)
			if err != nil {
				return commandPlan{}, errValueOutOfRange
			}
		}
		if !dict.Exists(key) {
			return staticIntegerPlan(0), nil
		}

		return commandPlan{
			write:       spec.write,
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
		key := string(args[1])
		expireAtMs, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return commandPlan{}, errValueOutOfRange
		}
		expireAtNano, err := expireAtNanoFromMs(expireAtMs)
		if err != nil {
			return commandPlan{}, errValueOutOfRange
		}
		if !dict.Exists(key) {
			return staticIntegerPlan(0), nil
		}

		return commandPlan{
			write:       spec.write,
			aofCommands: [][][]byte{makeCommand("PEXPIREAT", key, strconv.FormatInt(expireAtMs, 10))},
			exec: func() (interface{}, error) {
				if dict.ExpireAt(key, expireAtNano) {
					return int64(1), nil
				}
				return int64(0), nil
			},
		}, nil

	case "TTL":
		key := string(args[1])
		return commandPlan{
			exec: func() (interface{}, error) {
				return dict.TTL(key), nil
			},
		}, nil

	case "PTTL":
		key := string(args[1])
		return commandPlan{
			exec: func() (interface{}, error) {
				return dict.PTTL(key), nil
			},
		}, nil

	case "PERSIST":
		key := string(args[1])
		if !dict.HasExpire(key) {
			return staticIntegerPlan(0), nil
		}
		return commandPlan{
			write:       spec.write,
			aofCommands: [][][]byte{makeCommand("PERSIST", key)},
			exec: func() (interface{}, error) {
				if dict.Persist(key) {
					return int64(1), nil
				}
				return int64(0), nil
			},
		}, nil

	case "GET":
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
		keys := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys = append(keys, string(args[i]))
		}
		existingKeys := existingUniqueKeys(dict, keys)
		if len(existingKeys) == 0 {
			return staticIntegerPlan(0), nil
		}

		return commandPlan{
			write:       spec.write,
			aofCommands: [][][]byte{makeCommand(append([]string{"DEL"}, existingKeys...)...)},
			exec: func() (interface{}, error) {
				for _, key := range existingKeys {
					dict.Remove(key)
				}
				return len(existingKeys), nil
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

func copyCommand(args [][]byte) [][]byte {
	out := make([][]byte, 0, len(args))
	for _, arg := range args {
		argCopy := make([]byte, len(arg))
		copy(argCopy, arg)
		out = append(out, argCopy)
	}
	return out
}

func commandName(args [][]byte) string {
	if len(args) == 0 {
		return "<empty>"
	}
	return string(args[0])
}

func staticIntegerPlan(value int64) commandPlan {
	return commandPlan{
		exec: func() (interface{}, error) {
			return value, nil
		},
	}
}

func existingUniqueKeys(dict *datastruct.Dict, keys []string) []string {
	existing := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if dict.Exists(key) {
			existing = append(existing, key)
		}
	}
	return existing
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
	case "default", "all":
		return joinInfoSections(db.serverInfoBytes(), db.persistenceInfoBytes(), db.statsInfoBytes(), db.replicationInfoBytes()), nil
	case "server":
		return db.serverInfoBytes(), nil
	case "persistence":
		return db.persistenceInfoBytes(), nil
	case "stats":
		return db.statsInfoBytes(), nil
	case "replication":
		return db.replicationInfoBytes(), nil
	default:
		return nil, fmt.Errorf(errUnsupportedInfoFormat, section)
	}
}

func (db *Db) serverInfoBytes() []byte {
	uptimeSeconds := int64(time.Since(db.startTime).Seconds())
	if uptimeSeconds < 0 {
		uptimeSeconds = 0
	}

	lines := []string{
		"# Server",
		"server_name:redis-golang",
		"redis_mode:standalone",
		fmt.Sprintf("process_id:%d", os.Getpid()),
		fmt.Sprintf("go_version:%s", runtime.Version()),
		fmt.Sprintf("arch_bits:%d", strconv.IntSize),
		fmt.Sprintf("uptime_in_seconds:%d", uptimeSeconds),
		fmt.Sprintf("uptime_in_days:%d", uptimeSeconds/(24*60*60)),
		fmt.Sprintf("db_count:%d", len(db.dicts)),
	}

	return []byte(strings.Join(lines, "\r\n") + "\r\n")
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

func (db *Db) statsInfoBytes() []byte {
	lines := []string{
		"# Stats",
		fmt.Sprintf("total_commands_processed:%d", atomic.LoadInt64(&db.totalCommands)),
		fmt.Sprintf("active_expire_running:%d", boolToInt(atomic.LoadInt32(&db.activeExpireRunning) == 1)),
		fmt.Sprintf("active_expire_cycles:%d", atomic.LoadInt64(&db.activeExpireCycles)),
		fmt.Sprintf("active_expired_keys:%d", atomic.LoadInt64(&db.activeExpiredKeys)),
		fmt.Sprintf("active_expire_interval_ms:%d", atomic.LoadInt64(&db.activeExpireIntervalMs)),
		fmt.Sprintf("active_expire_limit_per_db:%d", atomic.LoadInt64(&db.activeExpireLimitPerDB)),
	}

	return []byte(strings.Join(lines, "\r\n") + "\r\n")
}

func joinInfoSections(sections ...[]byte) []byte {
	var b strings.Builder
	for i, section := range sections {
		if i > 0 {
			b.WriteString("\r\n")
		}
		b.Write(section)
	}
	return []byte(b.String())
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
