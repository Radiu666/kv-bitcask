package fio

const DatafilePerm = 0644 // 文件默认权限

// IOManager 抽象IO管理接口，接入不同的IO类型
type IOManager interface {
	// Read 从文件指定位置读取数据
	Read([]byte, int64) (int, error)

	// Write 写字符数组至文件中
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取文件大小
	Size() (int64, error)
}
