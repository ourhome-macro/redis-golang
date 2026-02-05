package datastruct

import (
	"container/list"
	"sync"
)

const DefaultCapacity = 64 * 1024 * 1024 // 64MB

type Dict struct {
	capacity int64
	nbytes   int64
	mu       sync.RWMutex
	data     map[string]*entity
	ll       *list.List
}

type Value interface {
	Len() int
}

type entity struct {
	key      string
	value    Value
	listElem *list.Element
}

func MakeDict() *Dict {
	return &Dict{
		data:     make(map[string]*entity),
		ll:       list.New(),
		capacity: DefaultCapacity,
	}
}

func MakeDictWithCapacity(capacity int64) *Dict {
	return &Dict{
		data:     make(map[string]*entity),
		ll:       list.New(),
		capacity: capacity,
	}
}

func (d *Dict) Get(key string) (Value, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if v, ok := d.data[key]; ok {
		d.ll.MoveToFront(v.listElem)
		return v.value, true
	}
	return nil, false
}

func (d *Dict) Set(key string, value Value) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if v, ok := d.data[key]; ok {
		// 已有 Key
		delta := int64(value.Len() - v.value.Len())
		d.nbytes += delta
		v.value = value
		d.ll.MoveToFront(v.listElem)
	} else {
		// 新增 Key
		ent := &entity{key: key, value: value}
		ent.listElem = d.ll.PushFront(ent)
		d.data[key] = ent
		d.nbytes += int64(len(key)) + int64(value.Len())
	}

	for d.capacity > 0 && d.nbytes > d.capacity {
		d.RemoveOldest()
	}
}

func (d *Dict) RemoveOldest() {
	elem := d.ll.Back()
	if elem != nil {
		d.ll.Remove(elem)
		ent := elem.Value.(*entity)
		delete(d.data, ent.key)
		d.nbytes -= int64(len(ent.key)) + int64(ent.value.Len())
	}
}

func (d *Dict) Remove(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if v, ok := d.data[key]; ok {
		d.ll.Remove(v.listElem)
		delete(d.data, v.key)
		d.nbytes -= int64(len(v.key)) + int64(v.value.Len())
	}
}

func (d *Dict) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.ll.Len()
}

func (d *Dict) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data = make(map[string]*entity)
	d.ll.Init()
	d.nbytes = 0
}
