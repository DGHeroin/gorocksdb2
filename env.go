package gorocksdb

/*
#include "rocksdb/c.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
*/
import "C"
import (
    "encoding/binary"
    "errors"
    "fmt"
    "time"
    "unsafe"
)

type (
    Env struct {
        db  *C.rocksdb_t
        opt *C.rocksdb_options_t
    }
    WriteOptions struct {
        Sync bool
        TTL  int
    }
    ReadOptions struct {
    }
)

func NewEnv(path string) (*Env, error) {
    var (
        cstr = C.CString(path)
        cErr *C.char
        opt  = C.rocksdb_options_create()
    )
    defer C.free(unsafe.Pointer(cstr))
    defer C.rocksdb_options_destroy(opt)

    C.rocksdb_options_set_create_if_missing(opt, 1)
    db := C.rocksdb_open(opt, cstr, &cErr)
    if cErr != nil {
        defer C.rocksdb_free(unsafe.Pointer(cErr))
        return nil, errors.New(C.GoString(cErr))
    }

    env := &Env{
        db: db,
    }
    return env, nil
}
func NewEnvTTL(path string, ttl int) (*Env, error) {
    var (
        cstr = C.CString(path)
        cErr *C.char
        opt  = C.rocksdb_options_create()
    )

    defer C.free(unsafe.Pointer(cstr))
    defer C.rocksdb_options_destroy(opt)
    C.rocksdb_options_set_create_if_missing(opt, 1)
    db := C.rocksdb_open_with_ttl(opt, cstr, C.int(ttl), &cErr)
    if cErr != nil {
        defer C.rocksdb_free(unsafe.Pointer(cErr))
        return nil, errors.New(C.GoString(cErr))
    }

    env := &Env{
        db: db,
    }
    return env, nil
}
func (e *Env) Close() error {
    C.rocksdb_close(e.db)
    return nil
}

func (e *Env) Put(key []byte, value []byte, opts ...func(*WriteOptions)) error {
    var (
        cKey         *C.char
        cVal         *C.char
        cErr         *C.char
        writeoptions = C.rocksdb_writeoptions_create()
        opt          WriteOptions
        header       []byte
    )
    for _, fn := range opts {
        fn(&opt)
    }
    C.rocksdb_writeoptions_set_sync(writeoptions, boolToChar(opt.Sync))
    if opt.TTL == 0 { // 没有ttl
        header = []byte{0}
    } else {
        header = []byte{1, 0, 0, 0, 0}
        binary.BigEndian.PutUint32(header[1:], uint32(time.Now().Add(time.Duration(opt.TTL)*time.Second).Unix()))
    }
    value = append(header, value...)
    cKey = byteToChar(key)
    cVal = byteToChar(value)

    defer C.rocksdb_writeoptions_destroy(writeoptions)
    C.rocksdb_put(e.db, writeoptions, cKey, C.size_t(len(key)), cVal, C.size_t(len(value)), &cErr)
    if cErr != nil {
        defer C.rocksdb_free(unsafe.Pointer(cErr))
        return errors.New(C.GoString(cErr))
    }
    return nil
}
func (e *Env) Get(key []byte) ([]byte, error) {
    var (
        cErr        *C.char
        cValLen     C.size_t
        cKey        = byteToChar(key)
        readoptions = C.rocksdb_readoptions_create()
    )
    defer C.rocksdb_readoptions_destroy(readoptions)
    cVal := C.rocksdb_get(e.db, readoptions, cKey, C.size_t(len(key)), &cValLen, &cErr)
    if cErr != nil {
        defer C.rocksdb_free(unsafe.Pointer(cErr))
        return nil, errors.New(C.GoString(cErr))
    }
    if cValLen == 0 {
        return nil, nil
    }
    rawData := charToByte(cVal, cValLen)
    switch rawData[0] {
    case 0:
        // no ttl
        cValLen = cValLen - 1
        return rawData[1:], nil
    case 1:
        // with ttl
        ttl := binary.BigEndian.Uint32(rawData[1:5])
        tTime := time.Unix(int64(ttl), 0)
        if time.Now().After(tTime) { // timeout
            _ = e.Delete(key)
            return nil, nil
        }
        return rawData[5:], nil
    }

    return nil, fmt.Errorf("data error")
}

func (e *Env) Delete(key []byte) error {

    var (
        cKey         = byteToChar(key)
        cErr         *C.char
        writeoptions = C.rocksdb_writeoptions_create()
    )
    defer C.rocksdb_writeoptions_destroy(writeoptions)
    C.rocksdb_delete(e.db, writeoptions, cKey, C.size_t(len(key)), &cErr)
    if cErr != nil {
        defer C.rocksdb_free(unsafe.Pointer(cErr))
        return errors.New(C.GoString(cErr))
    }
    return nil
}

func WithTTL(ttl int) func(*WriteOptions) {
    return func(o *WriteOptions) {
        o.TTL = ttl
    }
}
