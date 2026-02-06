package datastruct

import (
	"container/list"
	"sync"
	"time"
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
	expire   int64 //ms
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
		//惰性删除
		if v.expire > 0 && time.Now().UnixNano() > v.expire {
			d.ll.Remove(v.listElem)
			delete(d.data, v.key)
			d.nbytes -= int64(len(v.key)) + int64(v.value.Len())
			//delete(d.data, key)
			return nil, false
		} else {
			d.ll.MoveToFront(v.listElem)
			return v.value, true
		}
	}
	return nil, false
}

func (d *Dict) SetWithTTL(key string, value Value, ttl int64) {
	//ttl: ms
	d.mu.Lock()
	defer d.mu.Unlock()
	var expire int64
	if ttl > 0 {
		expire = time.Now().UnixNano() + ttl*1e6
	} else {
		expire = 0
	}
	if v, ok := d.data[key]; ok {
		// 已有 Key
		delta := int64(value.Len() - v.value.Len())
		d.nbytes += delta
		v.value = value
		d.ll.MoveToFront(v.listElem)
		v.expire = expire
	} else {
		// 新增 Key
		ent := &entity{
			key:    key,
			value:  value,
			expire: expire,
		}
		ent.listElem = d.ll.PushFront(ent)
		d.data[key] = ent
		d.nbytes += int64(len(key)) + int64(value.Len())
		//v.expire = expire
	}
	for d.capacity > 0 && d.nbytes > d.capacity {
		d.RemoveOldest()
	}
}

func (d *Dict) Set(key string, value Value) {
	d.SetWithTTL(key, value, 0)
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
