package data

import "kv-bitcask/fio"

// DataFile 数据文件，bitcask里面包括的主体，分为active和non-active
type DataFile struct {
	FileId    uint32        // 文件ID
	WriteOff  int64         // 文件写到了那个位置
	IOManager fio.IOManager // io读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Write(buf []byte) error {
	size, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(size)
	return nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, -1, nil
}
