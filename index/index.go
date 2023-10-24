package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-bitcask/data"
)

// Indexer 抽象索引接口，后续可接入其他索引结构
type Indexer interface {
	// Put 存储key，及其对应的数据存放位置
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 获得指定key对应的数据存放位置
	Get(key []byte) *data.LogRecordPos

	// Delete 获得指定key对应的数据存放位置
	Delete(key []byte) bool
}

type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return nil
	default:
		panic("unsupported index type")
	}
}

// Item 因为BTree中的插入删除查找需要自己实现一个结构来实现less function
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (a *Item) Less(b btree.Item) bool {
	return bytes.Compare(a.key, b.(*Item).key) == -1
}
