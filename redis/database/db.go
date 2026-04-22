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

const MaxNumber = 16

type Db struct {
	dicts []*datastruct.Dict
	aof   *aof.AOF
}

type commandPlan struct {
	write bool
	exec  func() (interface{}, error)
}

func MakeDbs() *Db {
	dicts := make([]*datastruct.Dict, MaxNumber)
	for i := 0; i < MaxNumber; i++ {
		dicts[i] = datastruct.MakeDict()
	}

	db := &Db{dicts: dicts}
	loadAOF(db)
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
	plan, err := db.makeCommandPlan(index, args)
	if err != nil {
		return nil, err
	}

	if plan.write && db.aof != nil {
		if err := db.aof.AppendCommand(index, args); err != nil {
			return nil, err
		}
	}

	return plan.exec()
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

		commands = append(commands, aof.RewriteCommand{Args: [][]byte{
			[]byte("SELECT"),
			[]byte(strconv.Itoa(dbIndex)),
		}})

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
				continue
			}

			commands = append(commands, aof.RewriteCommand{Args: [][]byte{
				[]byte("SET"),
				[]byte(item.Key),
				valCopy,
			}})
		}
	}

	return commands, nil
}

func (db *Db) makeCommandPlan(index int, args [][]byte) (commandPlan, error) {
	if len(args) == 0 {
		return commandPlan{}, errors.New("empty command")
	}

	cmd := strings.ToUpper(string(args[0]))
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
		return commandPlan{
			write: true,
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

		return commandPlan{
			write: true,
			exec: func() (interface{}, error) {
				dict.SetWithTTL(key, NewDataObject(value), ttl)
				return "OK", nil
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
			write: true,
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
