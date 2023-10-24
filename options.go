package kv_bitcask

import "kv-bitcask/index"

// Options 实现用户可自选的一些选项
type Options struct {
	DirPath      string          // 数据库数据目录
	DataFileSize int64           // 数据文件的大小
	SyncWrites   bool            // 是否每次写数据都进行持久化
	IndexType    index.IndexType // 索引类型
}
