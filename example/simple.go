package main

import (
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

    key := []byte("key")
    val := []byte("value")
    env.Put(key, val)
    {
       result, err := env.Get(key)
       log.Println(string(result), err)
    }
    env.Delete(key)
    {
       result, err := env.Get(key)
        log.Println(string(result), err)
    }
    env.Put(key, val, gorocksdb.WithTTL(2))
    {
        result, err := env.Get(key)
        log.Println(string(result), err)
    }
    time.Sleep(time.Second * 2)
    {
        result, err := env.Get(key)
        log.Println(string(result), err)
    }
}
