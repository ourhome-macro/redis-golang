package datastruct

import (
	"math/rand"
	"time"
)

const (
	MaxLevel    = 16
	Probability = 0.5
)

// SkipListNode 带有 span 的跳表节点
type SkipListNode struct {
	key     string
	score   float64
	forward []*SkipListNode
	span    []int // 记录该层指针跨越的节点数
}

// SkipList 跳表
type SkipList struct {
	header *SkipListNode
	level  int
	length int // 记录节点总数，方便计算
	rander *rand.Rand
}

func NewSkipList() *SkipList {
	return &SkipList{
		header: &SkipListNode{
			key:     "",
			score:   0,
			forward: make([]*SkipListNode, MaxLevel),
			span:    make([]int, MaxLevel),
		},
		level:  0,
		length: 0,
		rander: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func newNode(key string, score float64, level int) *SkipListNode {
	return &SkipListNode{
		key:     key,
		score:   score,
		forward: make([]*SkipListNode, level+1),
		span:    make([]int, level+1),
	}
}

// randomLevel 随机层数
func (sl *SkipList) randomLevel() int {
	level := 0
	for level < MaxLevel && sl.rander.Float64() < Probability {
		level++
	}
	return level
}

func (sl *SkipList) find(key string) *SkipListNode {
	current := sl.header
	for l := sl.level; l >= 0; l-- {
		for current.forward[l] != nil && current.forward[l].key < key {
			current = current.forward[l]
		}
	}
	target := current.forward[0]
	if target != nil && target.key == key {
		return target
	}
	return nil
}
