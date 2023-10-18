package data

// LogRecordPos 数据内存索引，描述数据在磁盘的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示数据存储在哪个文件
	Offset int64  // 偏移量，表示将数据存储在数据文件的那个位置
}
