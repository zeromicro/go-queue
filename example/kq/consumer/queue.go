package main

import (
    "fmt"

    "github.com/tal-tech/go-zero/core/conf"

    "github.com/tal-tech/go-queue/kq"
)

func main() {
    var c kq.KqConf
    conf.MustLoad("config.json", &c)

    q := kq.MustNewQueue(c, kq.WithHandle(func(k, v string) error {
        fmt.Printf("=> %s\n", v)
        return nil
    }))
    defer q.Stop()
    q.Start()
}
