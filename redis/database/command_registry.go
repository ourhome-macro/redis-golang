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
	argStep  int
	write    bool
	keyspace bool
	firstKey int
	lastKey  int
	keyStep  int
}

var commandRegistry = map[string]commandSpec{
	"PING":         {name: "PING", minArity: 1, maxArity: 2, argStep: 1},
	"ECHO":         {name: "ECHO", minArity: 2, maxArity: 2, argStep: 1},
	"INFO":         {name: "INFO", minArity: 1, maxArity: 2, argStep: 1},
	"SELECT":       {name: "SELECT", minArity: 2, maxArity: 2, argStep: 1},
	"BGREWRITEAOF": {name: "BGREWRITEAOF", minArity: 1, maxArity: 1, argStep: 1},

	"EXISTS": {name: "EXISTS", minArity: 2, maxArity: variableArity, argStep: 1, keyspace: true, firstKey: 1, lastKey: -1, keyStep: 1},
	"MGET":   {name: "MGET", minArity: 2, maxArity: variableArity, argStep: 1, keyspace: true, firstKey: 1, lastKey: -1, keyStep: 1},
	"GET":    {name: "GET", minArity: 2, maxArity: 2, argStep: 1, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
	"TTL":    {name: "TTL", minArity: 2, maxArity: 2, argStep: 1, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
	"PTTL":   {name: "PTTL", minArity: 2, maxArity: 2, argStep: 1, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},

	"SET":         {name: "SET", minArity: 3, maxArity: 3, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
	"MSET":        {name: "MSET", minArity: 3, maxArity: variableArity, argStep: 2, write: true, keyspace: true, firstKey: 1, lastKey: -1, keyStep: 2},
	"SETWITHTTL":  {name: "SETWITHTTL", minArity: 4, maxArity: 4, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
	"SETWITHPXAT": {name: "SETWITHPXAT", minArity: 4, maxArity: 4, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
	"EXPIRE":      {name: "EXPIRE", minArity: 3, maxArity: 3, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
	"PEXPIRE":     {name: "PEXPIRE", minArity: 3, maxArity: 3, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
	"PEXPIREAT":   {name: "PEXPIREAT", minArity: 3, maxArity: 3, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
	"PERSIST":     {name: "PERSIST", minArity: 2, maxArity: 2, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: 1, keyStep: 1},
	"DEL":         {name: "DEL", minArity: 2, maxArity: variableArity, argStep: 1, write: true, keyspace: true, firstKey: 1, lastKey: -1, keyStep: 1},
}

func commandSpecForArgs(args [][]byte) (commandSpec, error) {
	if len(args) == 0 {
		return commandSpec{}, errors.New("empty command")
	}

	name := normalizeCommandName(args[0])
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
	if s.argStep > 1 && argc > s.minArity && (argc-s.minArity)%s.argStep != 0 {
		return s.arityError()
	}
	return nil
}

func (s commandSpec) arityError() error {
	return fmt.Errorf("wrong number of arguments for '%s'", strings.ToLower(s.name))
}

func (s commandSpec) keyArgumentIndexes(argc int) []int {
	if !s.keyspace || s.firstKey <= 0 || s.keyStep <= 0 {
		return nil
	}

	last := s.lastKey
	if last < 0 {
		last = argc + last
	}
	if last < s.firstKey {
		return nil
	}

	indexes := make([]int, 0, 1+(last-s.firstKey)/s.keyStep)
	for idx := s.firstKey; idx <= last; idx += s.keyStep {
		indexes = append(indexes, idx)
	}
	return indexes
}

func normalizeCommandName(raw []byte) string {
	return strings.ToUpper(string(raw))
}
