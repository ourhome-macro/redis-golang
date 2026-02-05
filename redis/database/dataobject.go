package database

// DataObject 实际存储数据的结构体
// 它需要实现 datastruct.Value 接口
type DataObject struct {
	val []byte
}

// NewDataObject 创建一个数据对象
func NewDataObject(val []byte) *DataObject {
	return &DataObject{val: val}
}

// Len 实现 Value 接口：返回内存占用大小
func (o *DataObject) Len() int {
	return len(o.val)
}

// String 辅助方法：返回字符串形式
func (o *DataObject) String() string {
	return string(o.val)
}

// Bytes 辅助方法：返回字节数组
func (o *DataObject) Bytes() []byte {
	return o.val
}
