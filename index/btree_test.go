package index

import (
	"github.com/stretchr/testify/assert"
	"kv-bitcask/data"
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
