package datastruct

import (
	"math/rand"
	"time"
)

const (
	// 这里使用 32（Redis 默认值），也可按需扩展到 64。
	ZSkipListMaxLevel = 32
	// Redis 源码 zset.c: ZSKIPLIST_P
	ZSkipListP = 0.25
)

// Forward: 该层前进指针。
// Span: 从当前节点跨越多少个 level[0] 节点可到达 Forward（用于 O(logN) rank 查询）。
type SkipListLevel struct {
	Forward *SkipListNode
	Span    int64
}


// 必备字段：score/member/backward/level[]。
type SkipListNode struct {
	Score    float64
	Member   string
	Backward *SkipListNode
	Level    []SkipListLevel
}


// Level 表示当前跳表有效层数（最小为 1）。
type SkipList struct {
	header *SkipListNode
	tail   *SkipListNode
	level  int
	length int64
	rander *rand.Rand
}

// NewSkipList 创建一个空跳表。
// 头节点是哨兵节点，不承载真实 member。
func NewSkipList() *SkipList {
	header := &SkipListNode{
		Level: make([]SkipListLevel, ZSkipListMaxLevel),
	}

	return &SkipList{
		header: header,
		level:  1,
		length: 0,
		rander: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func newNode(level int, score float64, member string) *SkipListNode {
	return &SkipListNode{
		Score:  score,
		Member: member,
		Level:  make([]SkipListLevel, level),
	}
}

// randomLevel 对应 Redis 的 zslRandomLevel。
// 返回值范围为 [1, ZSkipListMaxLevel]。
func (sl *SkipList) randomLevel() int {
	level := 1
	for level < ZSkipListMaxLevel && sl.rander.Float64() < ZSkipListP {
		level++
	}
	return level
}

// Len 返回当前节点数。
func (sl *SkipList) Len() int64 {
	return sl.length
}

// Insert 对应 Redis 的 zslInsert。
// 排序规则：score 升序；score 相同按 member 字典序升序。
// 关键点：rank[] + span 计算，保证后续 GetRank/GetElementByRank 仍为 O(logN)。
func (sl *SkipList) Insert(score float64, member string) *SkipListNode {
	update := make([]*SkipListNode, ZSkipListMaxLevel)
	rank := make([]int64, ZSkipListMaxLevel)

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for x.Level[i].Forward != nil &&
			(x.Level[i].Forward.Score < score ||
				(x.Level[i].Forward.Score == score && x.Level[i].Forward.Member < member)) {
			rank[i] += x.Level[i].Span
			x = x.Level[i].Forward
		}
		update[i] = x
	}

	level := sl.randomLevel()
	if level > sl.level {
		// 新增高层时，header 在这些层直接跨越整个链表长度。
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].Level[i].Span = sl.length
		}
		sl.level = level
	}

	x = newNode(level, score, member)
	for i := 0; i < level; i++ {
		// 插入 forward 链
		x.Level[i].Forward = update[i].Level[i].Forward
		update[i].Level[i].Forward = x

		// Redis 同款 span 公式：
		// x.span = update.span - (rank[0] - rank[i])
		// update.span = (rank[0] - rank[i]) + 1
		x.Level[i].Span = update[i].Level[i].Span - (rank[0] - rank[i])
		update[i].Level[i].Span = (rank[0] - rank[i]) + 1
	}

	// 未参与插入的更高层，跨度+1（因为底层多了一个节点）。
	for i := level; i < sl.level; i++ {
		update[i].Level[i].Span++
	}

	if update[0] == sl.header {
		x.Backward = nil
	} else {
		x.Backward = update[0]
	}

	if x.Level[0].Forward != nil {
		x.Level[0].Forward.Backward = x
	} else {
		sl.tail = x
	}

	sl.length++
	return x
}

// Delete 对应 Redis 的 zslDelete + zslDeleteNode。
// 删除成功返回 true；否则返回 false。
func (sl *SkipList) Delete(score float64, member string) bool {
	update := make([]*SkipListNode, ZSkipListMaxLevel)
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil &&
			(x.Level[i].Forward.Score < score ||
				(x.Level[i].Forward.Score == score && x.Level[i].Forward.Member < member)) {
			x = x.Level[i].Forward
		}
		update[i] = x
	}

	x = x.Level[0].Forward
	if x == nil || x.Score != score || x.Member != member {
		return false
	}

	sl.deleteNode(x, update)
	return true
}

// deleteNode 仅在节点已确认存在时调用。
func (sl *SkipList) deleteNode(x *SkipListNode, update []*SkipListNode) {
	for i := 0; i < sl.level; i++ {
		if update[i].Level[i].Forward == x {
			// 删除命中层：update 跨度并入 x 的跨度，再减去 x 自身这个节点。
			update[i].Level[i].Span += x.Level[i].Span - 1
			update[i].Level[i].Forward = x.Level[i].Forward
		} else {
			// 未命中层：虽然 forward 不变，但底层节点少了 1。
			update[i].Level[i].Span--
		}
	}

	if x.Level[0].Forward != nil {
		x.Level[0].Forward.Backward = x.Backward
	} else {
		sl.tail = x.Backward
	}

	for sl.level > 1 && sl.header.Level[sl.level-1].Forward == nil {
		sl.level--
	}

	sl.length--
}

// GetRank 对应 Redis 的 zslGetRank。
// 返回 1-based 排名；若不存在返回 0。
func (sl *SkipList) GetRank(score float64, member string) int64 {
	var rank int64
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil &&
			(x.Level[i].Forward.Score < score ||
				(x.Level[i].Forward.Score == score && x.Level[i].Forward.Member <= member)) {
			rank += x.Level[i].Span
			x = x.Level[i].Forward
		}

		if x != sl.header && x.Score == score && x.Member == member {
			return rank
		}
	}

	return 0
}

// GetElementByRank 对应 Redis 的 zslGetElementByRank。
// rank 为 1-based，超出范围返回 nil。
func (sl *SkipList) GetElementByRank(rank int64) *SkipListNode {
	if rank <= 0 || rank > sl.length {
		return nil
	}

	var traversed int64
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil && traversed+x.Level[i].Span <= rank {
			traversed += x.Level[i].Span
			x = x.Level[i].Forward
		}
		if traversed == rank {
			return x
		}
	}

	return nil
}

// RangeByRank 返回 [start, end]（1-based, 闭区间）内的节点，按分值升序。
// 实现策略：
// 1) 先用 GetElementByRank(start) 借助 span 做 O(logN) 定位；
// 2) 再沿 level[0] 顺序扫描 O(M)（M 为返回条数）。
// 总复杂度 O(logN + M)。
func (sl *SkipList) RangeByRank(start, end int64) []*SkipListNode {
	if sl.length == 0 || start > end {
		return nil
	}

	if start < 1 {
		start = 1
	}
	if end > sl.length {
		end = sl.length
	}
	if start > end {
		return nil
	}

	res := make([]*SkipListNode, 0, end-start+1)
	x := sl.GetElementByRank(start)
	for rank := start; rank <= end && x != nil; rank++ {
		res = append(res, x)
		x = x.Level[0].Forward
	}

	return res
}

// TopN 返回得分最高的前 N 个节点（高分在前）。
// 注意：跳表默认是升序，因此这里先取尾部区间，再原地反转为降序。
// 当 score 相同时，默认顺序规则是 member 字典序升序；
// TopN 反转后会表现为 member 字典序降序（与“全局有序结果逆序”一致）。
func (sl *SkipList) TopN(n int64) []*SkipListNode {
	if n <= 0 || sl.length == 0 {
		return nil
	}
	if n > sl.length {
		n = sl.length
	}

	start := sl.length - n + 1
	res := sl.RangeByRank(start, sl.length)

	for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}

	return res
}
