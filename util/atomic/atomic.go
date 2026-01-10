package atomic

import "sync/atomic"

type Boolean uint32

// Get reads the value atomically
// 内存对齐
// 用指针骗过了编译器，就可以调用 LoadUint32 来原子的读取这 4 个字节。
// 这保证了即使有多个 Goroutine 同时在写这个值，读出来的也是完整的，不会读到一半（并发安全
func (b *Boolean) Get() bool {
	return atomic.LoadUint32((*uint32)(b)) != 0
}

// Set writes the value atomically

func (b *Boolean) Set(v bool) {
	if v {
		atomic.StoreUint32((*uint32)(b), 1)
	} else {
		atomic.StoreUint32((*uint32)(b), 0)
	}
}
