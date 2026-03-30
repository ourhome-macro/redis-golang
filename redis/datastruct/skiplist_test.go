package datastruct

import "testing"

func TestSkipListRankAndOrderWithSameScore(t *testing.T) {
	sl := NewSkipList()

	sl.Insert(1.0, "b")
	sl.Insert(1.0, "a")
	sl.Insert(2.0, "x")
	sl.Insert(1.0, "c")

	// 期望顺序：1.0-a, 1.0-b, 1.0-c, 2.0-x
	cases := []struct {
		rank   int64
		score  float64
		member string
	}{
		{1, 1.0, "a"},
		{2, 1.0, "b"},
		{3, 1.0, "c"},
		{4, 2.0, "x"},
	}

	for _, c := range cases {
		n := sl.GetElementByRank(c.rank)
		if n == nil {
			t.Fatalf("rank %d returned nil", c.rank)
		}
		if n.Score != c.score || n.Member != c.member {
			t.Fatalf("rank %d expected (%v,%s), got (%v,%s)", c.rank, c.score, c.member, n.Score, n.Member)
		}

		r := sl.GetRank(c.score, c.member)
		if r != c.rank {
			t.Fatalf("GetRank(%v,%s) expected %d, got %d", c.score, c.member, c.rank, r)
		}
	}
}

func TestSkipListDeleteAndRankUpdate(t *testing.T) {
	sl := NewSkipList()
	sl.Insert(1.0, "a")
	sl.Insert(2.0, "b")
	sl.Insert(3.0, "c")

	if !sl.Delete(2.0, "b") {
		t.Fatalf("delete existing node failed")
	}
	if sl.Delete(2.0, "b") {
		t.Fatalf("delete non-existing node should return false")
	}

	if sl.Len() != 2 {
		t.Fatalf("expected len=2, got %d", sl.Len())
	}
	if sl.GetRank(3.0, "c") != 2 {
		t.Fatalf("rank should be updated after delete")
	}
	if sl.GetElementByRank(3) != nil {
		t.Fatalf("out-of-range rank should return nil")
	}
}

func TestSkipListRangeByRank(t *testing.T) {
	sl := NewSkipList()
	sl.Insert(10, "a")
	sl.Insert(20, "b")
	sl.Insert(20, "c")
	sl.Insert(15, "d")
	sl.Insert(5, "e")

	// 升序全序应为：5-e, 10-a, 15-d, 20-b, 20-c
	r := sl.RangeByRank(2, 4)
	if len(r) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(r))
	}

	expected := []struct {
		score  float64
		member string
	}{
		{10, "a"},
		{15, "d"},
		{20, "b"},
	}

	for i := range expected {
		if r[i].Score != expected[i].score || r[i].Member != expected[i].member {
			t.Fatalf("range idx %d expected (%v,%s), got (%v,%s)", i, expected[i].score, expected[i].member, r[i].Score, r[i].Member)
		}
	}
}

func TestSkipListTopN(t *testing.T) {
	sl := NewSkipList()
	sl.Insert(10, "a")
	sl.Insert(20, "b")
	sl.Insert(20, "c")
	sl.Insert(15, "d")
	sl.Insert(5, "e")

	// Top3（高分优先）应为：20-c, 20-b, 15-d
	top3 := sl.TopN(3)
	if len(top3) != 3 {
		t.Fatalf("expected top3 len=3, got %d", len(top3))
	}

	expectedTop3 := []struct {
		score  float64
		member string
	}{
		{20, "c"},
		{20, "b"},
		{15, "d"},
	}

	for i := range expectedTop3 {
		if top3[i].Score != expectedTop3[i].score || top3[i].Member != expectedTop3[i].member {
			t.Fatalf("top3 idx %d expected (%v,%s), got (%v,%s)", i, expectedTop3[i].score, expectedTop3[i].member, top3[i].Score, top3[i].Member)
		}
	}

	// n 超过总量时，返回全量（降序）
	all := sl.TopN(99)
	if len(all) != int(sl.Len()) {
		t.Fatalf("expected all len=%d, got %d", sl.Len(), len(all))
	}
}
