package database

import (
	"errors"
	"fmt"
	"strings"
)

const variableArity = -1

type commandSpec struct {
	name     string
	minArity int
	maxArity int
	write    bool
	keyspace bool
}

var commandRegistry = map[string]commandSpec{
	"PING":         {name: "PING", minArity: 1, maxArity: 2},
	"ECHO":         {name: "ECHO", minArity: 2, maxArity: 2},
	"INFO":         {name: "INFO", minArity: 1, maxArity: 2},
	"SELECT":       {name: "SELECT", minArity: 2, maxArity: 2},
	"BGREWRITEAOF": {name: "BGREWRITEAOF", minArity: 1, maxArity: 1},

	"EXISTS": {name: "EXISTS", minArity: 2, maxArity: variableArity, keyspace: true},
	"MGET":   {name: "MGET", minArity: 2, maxArity: variableArity, keyspace: true},
	"GET":    {name: "GET", minArity: 2, maxArity: 2, keyspace: true},
	"TTL":    {name: "TTL", minArity: 2, maxArity: 2, keyspace: true},
	"PTTL":   {name: "PTTL", minArity: 2, maxArity: 2, keyspace: true},

	"SET":         {name: "SET", minArity: 3, maxArity: 3, write: true, keyspace: true},
	"MSET":        {name: "MSET", minArity: 3, maxArity: variableArity, write: true, keyspace: true},
	"SETWITHTTL":  {name: "SETWITHTTL", minArity: 4, maxArity: 4, write: true, keyspace: true},
	"SETWITHPXAT": {name: "SETWITHPXAT", minArity: 4, maxArity: 4, write: true, keyspace: true},
	"EXPIRE":      {name: "EXPIRE", minArity: 3, maxArity: 3, write: true, keyspace: true},
	"PEXPIRE":     {name: "PEXPIRE", minArity: 3, maxArity: 3, write: true, keyspace: true},
	"PEXPIREAT":   {name: "PEXPIREAT", minArity: 3, maxArity: 3, write: true, keyspace: true},
	"PERSIST":     {name: "PERSIST", minArity: 2, maxArity: 2, write: true, keyspace: true},
	"DEL":         {name: "DEL", minArity: 2, maxArity: variableArity, write: true, keyspace: true},
}

func commandSpecForArgs(args [][]byte) (commandSpec, error) {
	if len(args) == 0 {
		return commandSpec{}, errors.New("empty command")
	}

	name := strings.ToUpper(string(args[0]))
	spec, ok := commandRegistry[name]
	if !ok {
		return commandSpec{}, fmt.Errorf("unknown command '%s'", name)
	}
	if err := spec.validateArity(len(args)); err != nil {
		return commandSpec{}, err
	}
	return spec, nil
}

func (s commandSpec) validateArity(argc int) error {
	if argc < s.minArity {
		return s.arityError()
	}
	if s.maxArity != variableArity && argc > s.maxArity {
		return s.arityError()
	}
	return nil
}

func (s commandSpec) arityError() error {
	return fmt.Errorf("wrong number of arguments for '%s'", strings.ToLower(s.name))
}
