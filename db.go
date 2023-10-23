package kv_bitcask

import (
	"kv-bitcask/data"
	"kv-bitcask/index"
	"sync"
)

// DB bitcask存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []uint32
	activeFile *data.DataFile
	olderFile  map[uint32]*data.DataFile
	index      index.Indexer
}

// Put 写入key-val，key不为空，加锁在appendLogRecord中考虑
func (db *DB) Put(key []byte, value []byte) error {
	// 判断是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 创建LogRecord格式文件，即为行记录
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 插入到当前活跃active文件中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 添加内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get 根据key读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 判断key是否有效
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存数据结构获取key对应索引信息
	LogRecordPos := db.index.Get(key)
	// 看key是否在索引中
	if LogRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件ID找到对应的文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == LogRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFile[LogRecordPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移获得数据
	logRecord, _, err := dataFile.ReadLogRecord(LogRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 墓碑值
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// 追加写的方式，写入active文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前是否存在active文件，如果没有则需要进行初始化
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 进行序列化操作
	encRecord, size := data.EncodeLogRecord(logRecord)

	// 判断写入文件数据是否达到文件的阈值，如果是则关闭当前文件，新开一个页
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先持久化数据文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前文件加入到不活跃状态
		db.olderFile[db.activeFile.FileId] = db.activeFile

		// 开新页
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// 设置active文件
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
