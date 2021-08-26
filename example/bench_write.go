package main

import (
    "fmt"
    "github.com/DGHeroin/gorocksdb"
    "log"
    "time"
)

func main() {
    env, err := gorocksdb.NewEnvTTL("./admin.db", 2)
    if err != nil {
        log.Println(err)
        return
    }
    defer env.Close()
    var keys []string
    val := []byte("value")

    for i := 0; i < 1000*1000; i++ {
        keys = append(keys, fmt.Sprintf("key:%d", i))
    }

    startTime := time.Now()
    for _, key := range keys {
        env.Put([]byte(key), val)
    }
    log.Println("elapsed time:", time.Since(startTime))
}
