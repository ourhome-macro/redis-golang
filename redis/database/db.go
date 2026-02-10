package database

import (
	"MiddlewareSelf/redis/aof"
	"MiddlewareSelf/redis/datastruct"
	"MiddlewareSelf/redis/parser"
	"MiddlewareSelf/redis/resp"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
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
	file, _ := os.Open("redis.aof")
	defer file.Close()
	ch := parser.ParseStream(file)
	db := &Db{
		dicts: dicts,
	}

	for payLoad := range ch {
		if payLoad == nil {

			continue
		}
		if arr, ok := payLoad.Data.(*resp.ArrayReply); ok {
			_, exec := db.Exec(0, arr.Args)
			if exec != nil {
				return nil
			} // TODO 后续处理这个index问题
		}
	}
	return db
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
		respData := resp.MakeArrayReply(args).ToBytes()
		if db.aof != nil {
			_, _ = db.aof.File.Write(respData)
		}
	}

	return reply, nil
}
