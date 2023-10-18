package index

import (
	"github.com/google/btree"
	"kv-bitcask/data"
	"sync"
)

// BTree 索引，封装了google的 btree
type BTree struct {
	tree *btree.BTree
	// google的btree，多进程写是不安全的，需要加锁
	lock *sync.RWMutex
}

// NewBTree 新建BTree索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}
func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	btreeItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if btreeItem == nil {
		return false
	}
	return true
}
