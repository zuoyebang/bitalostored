# LocalCache

Golang本地缓存库，支持LFU、LRU及ARC淘汰算法。
## 特性
* 自动分片，提高并发性能
* 支持过期缓存、LFU、LRU及ARC淘汰算法。
* 线程安全。
* 支持事件回调，包括清除、删除、添加缓存。
* 支持配置自动加载缓存，如果不存在则自动加载数据。

## 安装
```
$ go get git.zuoyebang.cc/stored-bitalosdb/butils/localcache
```

## 推荐用法

### 使用全局缓存对象
```go
var GlobalCache = localcache.New(10 << 10).
    LRU().
    LoaderExpireFunc(loaderFunc).
    EvictedFunc(evictedFunc). // 可选
    Build()

// 统一维护公共获取数据的函数
var loaderFunc = func(key interface{}) (interface{}, *time.Duration, error) {
    if k, ok := key.(string); ok {
        // 业务1
        if strings.HasPrefix(k, "prefix1_") {
            v := "get from db"
            return v, nil, nil // 过期时间为nil，不设置过期
        }

        // 业务2
        if strings.HasPrefix(k, "prefix2_") {
            v := "get from redis"
            ex := 5*time.Second
            return v, &ex, nil // 过期时间为5s
        }
    }

    return nil, nil, errors.New("not support")
}

// 不想被淘汰的key重新刷新, 可选
var evictedFunc = func(key, value interface{}) {
    if k, ok := key.(string); ok {
        if strings.HasPrefix(k, "myPrefix_") {
            v := "get value"
            GlobalCache.Set(key, v)
        }
    }
}

// 业务1中使用
func Biz1() {
  key := fmt.Sprintf("prefix1_%s", param)
  value, err := GlobalCache.Get(key)
  if err != nil {
    handler(err)
  }
}
```

### 使用局部变量
使用局部变量需要对缓存的使用大小做到心里有数，严格控制数量
```go
var myCache = localcache.New(100).
    LRU().
    LoaderExpireFunc(loaderFunc).
    Build()

// 统一维护公共获取数据的函数
var loaderFunc = func(key interface{}) (interface{}, *time.Duration, error) {
    v := "get from db"
    return v, nil, nil // 过期时间为nil，不设置过期
}

// 业务中使用
func Biz() {
  value, err := myCache.Get("key")
  if err != nil {
    handler(err)
  }
}
```


## 其他例子(不建议使用，在可控范围内酌情使用)

### 手动设置键值对。
```go
package main

import (
  "github.com/zuoyebang/bitalostored/butils/localcache"
  "fmt"
)

func main() {
  gc := localcache.New(20).
    LRU().
    Build()
  gc.Set("key", "ok")
  value, err := gc.Get("key")
  if err != nil {
    panic(err)
  }
  fmt.Println("Get:", value)
}
```

```
Get: ok
```

### 手动设置键值对，带过期时间。
```go
package main

import (
  "github.com/zuoyebang/bitalostored/butils/localcache"
  "fmt"
  "time"
)

func main() {
  gc := localcache.New(20).
    LRU().
    Build()
  gc.SetWithExpire("key", "ok", time.Second*10)
  value, _ := gc.Get("key")
  fmt.Println("Get:", value)

  // Wait for value to expire
  time.Sleep(time.Second*10)

  value, err := gc.Get("key")
  if err != nil {
    panic(err)
  }
  fmt.Println("Get:", value)
}
```

```
Get: ok
// 10 seconds later, new attempt:
panic: ErrKeyNotFound
```

### 自动加载值

```go
package main

import (
  "github.com/zuoyebang/bitalostored/butils/localcache"
  "fmt"
)

func main() {
  gc := localcache.New(20).
    LRU().
    LoaderFunc(func(key interface{}) (interface{}, error) {
      return "ok", nil
    }).
    Build()
  value, err := gc.Get("key")
  if err != nil {
    panic(err)
  }
  fmt.Println("Get:", value)
}
```

```
Get: ok
```
### 自动加载值，带过期时间

```go
package main

import (
  "fmt"
  "time"

  "github.com/zuoyebang/bitalostored/butils/localcache"
)

func main() {
  var evictCounter, loaderCounter, purgeCounter int
  gc := localcache.New(20).
    LRU().
    LoaderExpireFunc(func(key interface{}) (interface{}, *time.Duration, error) {
      loaderCounter++
      expire := 1 * time.Second
      return "ok", &expire, nil
    }).
    EvictedFunc(func(key, value interface{}) {
      evictCounter++
      fmt.Println("evicted key:", key)
    }).
    PurgeVisitorFunc(func(key, value interface{}) {
      purgeCounter++
      fmt.Println("purged key:", key)
    }).
    Build()
  value, err := gc.Get("key")
  if err != nil {
    panic(err)
  }
  fmt.Println("Get:", value)
  time.Sleep(1 * time.Second)
  value, err = gc.Get("key")
  if err != nil {
    panic(err)
  }
  fmt.Println("Get:", value)
  gc.Purge()
  if loaderCounter != evictCounter+purgeCounter {
    panic("bad")
  }
}
```

```
Get: ok
evicted key: key
Get: ok
purged key: key
```

## 缓存淘汰算法

  * Least-Frequently Used (LFU)

  实现了LFU算法，基于访问次数，移除访问次数最少的元素。

  ```go
  func main() {
    // size: 10
    gc := localcache.New(10).
      LFU().
      Build()
    gc.Set("key", "value")
  }
  ```

  * Least Recently Used (LRU)

  实现了LRU算法，基于访问时间，移除访问时间最长的元素。

  ```go
  func main() {
    // size: 10
    gc := localcache.New(10).
      LRU().
      Build()
    gc.Set("key", "value")
  }
  ```

  * Adaptive Replacement Cache (ARC)

  自适应替换缓存，是对LRU和LFU的一种改进。

  ```go
  func main() {
    // size: 10
    gc := localcache.New(10).
      ARC().
      Build()
    gc.Set("key", "value")
  }
  ```

  * SimpleCache (Default)

  没有缓存淘汰的算法，简单的缓存。

  ```go
  func main() {
    // size: 10
    gc := localcache.New(10).Build()
    gc.Set("key", "value")
    v, err := gc.Get("key")
    if err != nil {
      panic(err)
    }
  }
  ```

## 加载缓存

如果指定了加载方法`LoaderFunc`，值将自动加载到缓存中，直到缓存被淘汰或手动失效。

```go
func main() {
  gc := localcache.New(10).
    LRU().
    LoaderFunc(func(key interface{}) (interface{}, error) {
      return "value", nil
    }).
    Build()
  v, _ := gc.Get("key")
  // output: "value"
  fmt.Println(v)
}
```

## 缓存过期

```go
func main() {
  // LRU cache, size: 10, expiration: after a hour
  gc := localcache.New(10).
    LRU().
    Expiration(time.Hour).
    Build()
}
```

## 事件回调

### 淘汰回调

```go
func main() {
  gc := localcache.New(2).
    EvictedFunc(func(key, value interface{}) {
      fmt.Println("evicted key:", key)
    }).
    Build()
  for i := 0; i < 3; i++ {
    gc.Set(i, i*i)
  }
}
```

```
evicted key: 0
```

### 添加事件回调

```go
func main() {
  gc := localcache.New(2).
    AddedFunc(func(key, value interface{}) {
      fmt.Println("added key:", key)
    }).
    Build()
  for i := 0; i < 3; i++ {
    gc.Set(i, i*i)
  }
}
```

```
added key: 0
added key: 1
added key: 2
```

## 性能测试结果

### 基本写入性能 (无分片非并发)

| 缓存类型 | 大小 1000 | 大小 10000 | 大小 100000 |
|---------|-----------|------------|-------------|
| Simple  | 275.4 ns/op | 329.7 ns/op | 538.6 ns/op |
| LRU     | 142.3 ns/op | 160.2 ns/op | 257.1 ns/op |
| ARC     | 212.2 ns/op | 246.5 ns/op | 467.1 ns/op |
| LFU     | 232.5 ns/op | 315.6 ns/op | 637.8 ns/op |

### 基本读取性能 (无分片非并发)

| 缓存类型 | 大小 1000 | 大小 10000 | 大小 100000 |
|---------|-----------|------------|-------------|
| Simple  | 53.30 ns/op | 66.03 ns/op | 78.26 ns/op |
| LRU     | 64.55 ns/op | 80.73 ns/op | 106.2 ns/op |
| ARC     | 70.59 ns/op | 79.64 ns/op | 137.3 ns/op |
| LFU     | 126.1 ns/op | 135.8 ns/op | 217.5 ns/op |

### 并行写入性能 (ParallelWrite)

| 缓存类型 | 大小 1000 | 大小 10000 | 大小 100000 |
|---------|-----------|------------|-------------|
| Simple  | 153.7 ns/op | 128.7 ns/op | 88.66 ns/op |
| LRU     | 108.8 ns/op | 84.63 ns/op | 74.83 ns/op |
| ARC     | 137.0 ns/op | 90.11 ns/op | 81.85 ns/op |
| LFU     | 137.1 ns/op | 128.2 ns/op | 114.5 ns/op |

### 并行读取性能 (ParallelRead)

| 缓存类型 | 大小 1000 | 大小 10000 | 大小 100000 |
|---------|-----------|------------|-------------|
| Simple  | 58.64 ns/op | 60.81 ns/op | 56.98 ns/op |
| LRU     | 72.32 ns/op | 77.07 ns/op | 75.07 ns/op |
| ARC     | 70.99 ns/op | 73.65 ns/op | 79.29 ns/op |
| LFU     | 98.72 ns/op | 105.4 ns/op | 101.2 ns/op |

### 混合操作性能 (Mixed)

| 缓存类型 | 大小 1000 | 大小 10000 | 大小 100000 |
|---------|-----------|------------|-------------|
| Simple  | 58.00 ns/op | 61.40 ns/op | 64.96 ns/op |
| LRU     | 66.44 ns/op | 68.21 ns/op | 74.02 ns/op |
| ARC     | 66.31 ns/op | 71.05 ns/op | 82.09 ns/op |
| LFU     | 69.13 ns/op | 74.84 ns/op | 83.03 ns/op |
