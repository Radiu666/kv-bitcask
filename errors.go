package kv_bitcask

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("key is empty")
	ErrIndexUpdateFailed = errors.New("index update failed")
	ErrKeyNotFound       = errors.New("key is not found")
	ErrDataFileNotFound  = errors.New("data file is not found")
)
