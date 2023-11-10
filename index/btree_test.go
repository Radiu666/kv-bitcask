package index

import (
	"github.com/stretchr/testify/assert"
	"kv-bitcask/data"
	"log"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	// 选择github上的testify库的assert
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 110})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()
	bt.Put([]byte("uu"), &data.LogRecordPos{Fid: 1, Offset: 100})
	bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 110})

	res1 := bt.Get([]byte("uu"))
	assert.Equal(t, uint32(1), res1.Fid)
	assert.Equal(t, int64(100), res1.Offset)

	res2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, int64(110), res2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	bt.Put([]byte("uu"), &data.LogRecordPos{Fid: 1, Offset: 100})
	bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 110})

	res1 := bt.Delete([]byte("uu"))
	assert.True(t, res1)
	res2 := bt.Delete([]byte("a"))
	assert.True(t, res2)
	res3 := bt.Delete([]byte("a"))
	assert.False(t, res3)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	// Btree为空
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// Btree有数据
	bt1.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	// log.Print(string(iter2.Key()))
	assert.NotNil(t, iter2.Key())
	// log.Print(iter2.Value())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 多条数据
	bt1.Put([]byte("acee"), &data.LogRecordPos{Fid: 2, Offset: 10})
	bt1.Put([]byte("eede"), &data.LogRecordPos{Fid: 2, Offset: 10})
	bt1.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 2, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
		//log.Print(string(iter3.Key()))
	}

	// 逆序
	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
		//log.Print(string(iter4.Key()))
	}

	// Seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
		//log.Print(string(iter5.Key()))
	}

	// 逆序seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("cc")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
		log.Print(string(iter6.Key()))
	}
}
