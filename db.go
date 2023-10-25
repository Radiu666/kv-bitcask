package kv_bitcask

import (
	"io"
	"kv-bitcask/data"
	"kv-bitcask/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int // 仅用于一开始加载实例
	activeFile *data.DataFile
	olderFile  map[uint32]*data.DataFile
	index      index.Indexer
}

// Open 根据配置项打开一个DB实例
func Open(options Options) (*DB, error) {
	// 校验配置项options
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断目录是否存在，不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化DB实例
	db := &DB{
		options:   options,
		mu:        new(sync.RWMutex),
		olderFile: make(map[uint32]*data.DataFile),
		index:     index.NewIndexer(options.IndexType),
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 加载索引文件
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
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

// Delete 根据key删除对应的数据
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 判断key是否存在，不存在则不添加新的记录进去
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造LogRecord，标记墓碑值
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}

	// 写入
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 从内存索引中删除
	ok := db.index.Delete(key)
	if !ok {
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

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中所有文件，找到所有以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitName[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件id进行排序，从小到大加载
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每个文件id，打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		// 最后一个是active文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else { // 否则加入到older file map中
			db.olderFile[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
func (db *DB) loadIndexFromDataFiles() error {
	// 如果没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 遍历文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFile[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				// 判断是否是读完该文件
				if err == io.EOF {
					break
				}
				return err
			}

			// 构建内存索引并保存
			LogRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			var ok bool
			if logRecord.Type == data.LogRecordDeleted {
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, LogRecordPos)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}
			offset += size
		}
		// 判断是active文件，则更新该文件的WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}

// 查验options是否合规
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return ErrDirPathIsEmpty
	}
	if options.DataFileSize <= 0 {
		return ErrFileSizeIllegal
	}
	return nil
}
