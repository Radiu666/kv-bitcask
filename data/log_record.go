package data

import "encoding/binary"

// LogRecordType 描述该行记录是否应被删除
type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord头部信息
// crc	 type	keySize	valueSize
//
//	4      1      5         5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

type LogRecordHeader struct {
	crc        uint32        // crc校验码
	recordType LogRecordType // 类型记录
	keySize    uint32
	valueSize  uint32
}

// LogRecordPos 数据内存索引，描述数据在磁盘的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示数据存储在哪个文件
	Offset int64  // 偏移量，表示将数据存储在数据文件的那个位置
}

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, -1
}

func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, -1
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
