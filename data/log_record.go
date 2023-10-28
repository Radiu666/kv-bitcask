package data

import (
	"encoding/binary"
	"hash/crc32"
)

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

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）        变长           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化Header字节数组
	header := make([]byte, maxLogRecordHeaderSize)
	header[4] = logRecord.Type

	var index = 5
	// 从index开始存放的是keySize和valueSize
	// 选择使用变长类型节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	// 最终生成的字节数组的大小
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	// 把header拷贝进来
	copy(encBytes[:index], header[:index])
	// 把key value 拷贝进来
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对LogRecord进行CRC校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// DecodeLogRecordHeader 对字节数组的头部进行解码
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	// 取出key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取出value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
