package database

import (
	"MiddlewareSelf/redis/datastruct"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

// TODO 完成每一个db隔离
const MaxNumber = 16

type Db struct {
	index int64
	dicts []*datastruct.Dict
}

func MakeDbs() *Db {
	dicts := make([]*datastruct.Dict, MaxNumber)
	for i := 0; i < MaxNumber; i++ {
		dicts[i] = datastruct.MakeDict()
	}
	return &Db{
		index: 0,
		dicts: dicts,
	}
}

func (db *Db) Select(index int) bool {
	if index < 0 || index >= MaxNumber {
		return false
	}
	atomic.StoreInt64(&db.index, int64(index))
	return true
}

func (db *Db) GetDict() *datastruct.Dict {
	idx := atomic.LoadInt64(&db.index)
	return db.dicts[int(idx)]
}

func (db *Db) Exec(args [][]byte) (interface{}, error) {
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
		index, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return nil, errors.New("invalid index argument")
		}
		if db.Select(index) {
			return "OK", nil
		}
		return nil, errors.New("DB index out of range")
	}

	dict := db.GetDict()

	switch cmd {
	case "SET":
		if len(args) != 3 {
			return nil, errors.New("wrong number of arguments for 'set'")
		}
		key := string(args[1])
		// 修正：直接把 []byte 包装成 DataObject
		val := NewDataObject(args[2])

		dict.Set(key, val)
		return "OK", nil

	case "GET":
		if len(args) != 2 {
			return nil, errors.New("wrong number of arguments for 'get'")
		}
		key := string(args[1])

		val, ok := dict.Get(key)
		if !ok {
			return nil, nil
		}

		// 断言
		if dobj, ok := val.(*DataObject); ok {
			return dobj.Bytes(), nil
		}
		return nil, errors.New("type assertion failed")

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
		return count, nil

	default:
		return nil, fmt.Errorf("unknown command '%s'", cmd)
	}
}
