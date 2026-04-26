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
	expire   int64 // absolute UnixNano, 0 means persistent
}

type SnapshotItem struct {
	Key          string
	Value        Value
	ExpireAtNano int64
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

	v, ok := d.getEntityLocked(key, true)
	if !ok {
		return nil, false
	}
	return v.value, true
}

func (d *Dict) SetWithTTL(key string, value Value, ttlMillis int64) {
	expire := int64(0)
	if ttlMillis > 0 {
		expire = time.Now().UnixNano() + ttlMillis*1e6
	}
	d.SetWithExpireAt(key, value, expire)
}

func (d *Dict) SetWithExpireAt(key string, value Value, expireAtNano int64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if expireAtNano > 0 && time.Now().UnixNano() >= expireAtNano {
		if v, ok := d.data[key]; ok {
			d.deleteEntityLocked(v)
		}
		return
	}

	if v, ok := d.data[key]; ok {
		delta := int64(value.Len() - v.value.Len())
		d.nbytes += delta
		v.value = value
		v.expire = expireAtNano
		d.ll.MoveToFront(v.listElem)
	} else {
		ent := &entity{
			key:    key,
			value:  value,
			expire: expireAtNano,
		}
		ent.listElem = d.ll.PushFront(ent)
		d.data[key] = ent
		d.nbytes += int64(len(key)) + int64(value.Len())
	}

	for d.capacity > 0 && d.nbytes > d.capacity {
		d.removeOldestLocked()
	}
}

func (d *Dict) Set(key string, value Value) {
	d.SetWithTTL(key, value, 0)
}

func (d *Dict) Expire(key string, ttlMillis int64) bool {
	if ttlMillis <= 0 {
		return d.ExpireAt(key, time.Now().UnixNano())
	}
	return d.ExpireAt(key, time.Now().UnixNano()+ttlMillis*1e6)
}

func (d *Dict) ExpireAt(key string, expireAtNano int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	v, ok := d.getEntityLocked(key, true)
	if !ok {
		return false
	}
	if expireAtNano <= time.Now().UnixNano() {
		d.deleteEntityLocked(v)
		return true
	}

	v.expire = expireAtNano
	return true
}

func (d *Dict) Persist(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	v, ok := d.getEntityLocked(key, true)
	if !ok {
		return false
	}
	if v.expire == 0 {
		return false
	}

	v.expire = 0
	return true
}

func (d *Dict) PTTL(key string) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	v, ok := d.getEntityLocked(key, false)
	if !ok {
		return -2
	}
	if v.expire == 0 {
		return -1
	}

	ttlMs := (v.expire - time.Now().UnixNano()) / 1e6
	if ttlMs < 0 {
		ttlMs = 0
	}
	return ttlMs
}

func (d *Dict) TTL(key string) int64 {
	ttlMs := d.PTTL(key)
	if ttlMs < 0 {
		return ttlMs
	}
	return ttlMs / 1000
}

func (d *Dict) RemoveOldest() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.removeOldestLocked()
}

func (d *Dict) Remove(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if v, ok := d.data[key]; ok {
		d.deleteEntityLocked(v)
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

func (d *Dict) PreviewSetEvictions(key string, valueLen int) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.capacity <= 0 {
		return nil
	}

	nextBytes := d.nbytes
	if ent, ok := d.data[key]; ok {
		nextBytes += int64(valueLen - ent.value.Len())
	} else {
		nextBytes += int64(len(key)) + int64(valueLen)
	}
	if nextBytes <= d.capacity {
		return nil
	}

	evicted := make([]string, 0)
	for elem := d.ll.Back(); elem != nil && nextBytes > d.capacity; elem = elem.Prev() {
		ent := elem.Value.(*entity)
		if ent.key == key {
			continue
		}
		evicted = append(evicted, ent.key)
		nextBytes -= int64(len(ent.key)) + int64(ent.value.Len())
	}
	if nextBytes > d.capacity {
		evicted = append(evicted, key)
	}
	return evicted
}

// Snapshot returns a read-only snapshot for AOF rewrite.
func (d *Dict) Snapshot() []SnapshotItem {
	d.mu.RLock()
	defer d.mu.RUnlock()

	now := time.Now().UnixNano()
	items := make([]SnapshotItem, 0, len(d.data))
	for _, ent := range d.data {
		if ent.expire > 0 && now > ent.expire {
			continue
		}
		items = append(items, SnapshotItem{
			Key:          ent.key,
			Value:        ent.value,
			ExpireAtNano: ent.expire,
		})
	}

	return items
}

func (d *Dict) getEntityLocked(key string, touch bool) (*entity, bool) {
	v, ok := d.data[key]
	if !ok {
		return nil, false
	}
	if v.expire > 0 && time.Now().UnixNano() > v.expire {
		d.deleteEntityLocked(v)
		return nil, false
	}
	if touch {
		d.ll.MoveToFront(v.listElem)
	}
	return v, true
}

func (d *Dict) deleteEntityLocked(v *entity) {
	d.ll.Remove(v.listElem)
	delete(d.data, v.key)
	d.nbytes -= int64(len(v.key)) + int64(v.value.Len())
}

func (d *Dict) removeOldestLocked() {
	elem := d.ll.Back()
	if elem == nil {
		return
	}
	ent := elem.Value.(*entity)
	d.deleteEntityLocked(ent)
}
