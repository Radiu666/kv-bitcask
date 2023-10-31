package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().UnixNano())) // 根据此时时间作为种子生成随机数
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey 生成测试用的Key
func GetTestKey(n int) []byte {
	return []byte(fmt.Sprintf("bitcask-key-%09d", n))
}

func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("bitcask-value-" + string(b))
}
