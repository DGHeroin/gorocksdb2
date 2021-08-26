package gorocksdb
import "C"
import (
    "reflect"
    "unsafe"
)

func byteToChar(b []byte) *C.char {
    var c *C.char
    if len(b) > 0 {
        c = (*C.char)(unsafe.Pointer(&b[0]))
    }
    return c
}

func charToByte(data *C.char, len C.size_t) []byte {
    var value []byte
    if len == 0 {
        return nil
    }

    sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
    sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))
    return value
}

func boolToChar(b bool) C.uchar {
    if b {
        return 1
    }
    return 0
}
