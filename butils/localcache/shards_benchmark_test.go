package localcache

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// 辅助函数
func buildBenchmarkShardsCache(size int) Cache {
	return New(size).
		Simple().Build()
}

func buildBenchmarkLRUCache(size int) Cache {
	return New(size).
		LRU().Build()
}

func buildBenchmarkLFUCache(size int) Cache {
	return New(size).
		LFU().Build()
}

func buildBenchmarkARCCache(size int) Cache {
	return New(size).
		ARC().Build()
}

func buildBenchmarkLoadingShardsCache(size int, loader LoaderFunc) Cache {
	return New(size).
		LoaderFunc(loader).
		Build()
}

func buildBenchmarkShardsCacheWithExpiration(size int, expiration time.Duration) Cache {
	return New(size).
		Expiration(expiration).
		Build()
}

// 预生成测试数据
type testData struct {
	key   string
	value interface{}
}

// 生成测试数据
func generateTestData(size int) []testData {
	data := make([]testData, size)
	for i := 0; i < size; i++ {
		data[i] = testData{
			key:   fmt.Sprintf("key-%d", i),
			value: fmt.Sprintf("value-%d", i),
		}
	}
	return data
}

// 生成测试数据并重置计时器
func generateTestDataAndReset(b *testing.B, size int) []testData {
	b.StopTimer()
	data := generateTestData(size)
	b.StartTimer()
	return data
}

// 生成指定大小的字符串
func generateString(size int) string {
	bytes := make([]byte, size)
	for i := 0; i < size; i++ {
		bytes[i] = byte(i%26 + 'a')
	}
	return string(bytes)
}

// 生成指定大小的字节数组
func generateBytes(size int) []byte {
	bytes := make([]byte, size)
	for i := 0; i < size; i++ {
		bytes[i] = byte(i % 256)
	}
	return bytes
}

// 生成指定大小的结构体
type LargeStruct struct {
	Data []byte
}

func generateStruct(size int) LargeStruct {
	return LargeStruct{
		Data: generateBytes(size),
	}
}

// 基准测试：基本设置操作
func BenchmarkShardsSet(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			cache := buildBenchmarkShardsCache(size)
			data := generateTestDataAndReset(b, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cache.Set(data[i].key, data[i].value)
			}
		})
	}
}

// 基准测试：基本获取操作
func BenchmarkShardsGet(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			cache := buildBenchmarkShardsCache(size)
			data := generateTestDataAndReset(b, size)
			for i := 0; i < size; i++ {
				cache.Set(data[i].key, data[i].value)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cache.Get(data[i%size].key)
			}
		})
	}
}

// 基准测试：并发设置操作
func BenchmarkShardsSetParallel(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			cache := buildBenchmarkShardsCache(size)
			data := generateTestDataAndReset(b, b.N)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					cache.Set(data[i].key, data[i].value)
					i = (i + 1) % b.N
				}
			})
		})
	}
}

// 基准测试：并发获取操作
func BenchmarkShardsGetParallel(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			cache := buildBenchmarkShardsCache(size)
			data := generateTestDataAndReset(b, size)
			for i := 0; i < size; i++ {
				cache.Set(data[i].key, data[i].value)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					cache.Get(data[i].key)
					i = (i + 1) % size
				}
			})
		})
	}
}

// 基准测试：带过期时间的设置操作
func BenchmarkShardsSetWithExpire(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			cache := buildBenchmarkShardsCacheWithExpiration(size, time.Second)
			data := generateTestDataAndReset(b, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cache.Set(data[i].key, data[i].value)
			}
		})
	}
}

// 基准测试：带加载器的获取操作
func BenchmarkShardsGetWithLoader(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			data := generateTestDataAndReset(b, size)
			loader := LoaderFunc(func(key interface{}) (interface{}, error) {
				return fmt.Sprintf("loaded-value-%s", key), nil
			})
			cache := buildBenchmarkLoadingShardsCache(size, loader)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cache.Get(data[i%size].key)
			}
		})
	}
}

// 基准测试：缓存命中率
func BenchmarkShardsHitRate(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			cache := buildBenchmarkShardsCache(size)
			data := generateTestDataAndReset(b, size)
			// 预填充缓存
			for i := 0; i < size/2; i++ {
				cache.Set(data[i].key, data[i].value)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 50%命中率
				if i%2 == 0 {
					cache.Get(data[i%size].key)
				} else {
					cache.Get(fmt.Sprintf("key-%d", i))
				}
			}
		})
	}
}

// 基准测试：缓存清理操作
func BenchmarkShardsPurge(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			cache := buildBenchmarkShardsCache(size)
			data := generateTestDataAndReset(b, size)
			for i := 0; i < size; i++ {
				cache.Set(data[i].key, data[i].value)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cache.Purge()
			}
		})
	}
}

// 基准测试：并行读写操作
func BenchmarkShardsParallelReadWrite(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	ratios := []struct {
		name      string
		readRatio float64
	}{
		{"read80_write20", 0.8},
		{"read50_write50", 0.5},
		{"read20_write80", 0.2},
	}

	for _, size := range sizes {
		for _, ratio := range ratios {
			b.Run(fmt.Sprintf("size_%d_%s", size, ratio.name), func(b *testing.B) {
				cache := buildBenchmarkShardsCache(size)
				data := generateTestDataAndReset(b, b.N)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						if rand.Float64() < ratio.readRatio {
							cache.Get(data[i%size].key)
						} else {
							cache.Set(data[i].key, data[i].value)
						}
						i = (i + 1) % b.N
					}
				})
			})
		}
	}
}

// 基准测试：高并发读写操作
func BenchmarkShardsHighConcurrency(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	concurrencies := []int{2, 4, 8, 16, 32}

	for _, size := range sizes {
		for _, concurrency := range concurrencies {
			b.Run(fmt.Sprintf("size_%d_concurrency_%d", size, concurrency), func(b *testing.B) {
				cache := buildBenchmarkShardsCache(size)
				data := generateTestDataAndReset(b, b.N)
				b.SetParallelism(concurrency)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						if i%2 == 0 {
							cache.Get(data[i%size].key)
						} else {
							cache.Set(data[i].key, data[i].value)
						}
						i = (i + 1) % b.N
					}
				})
			})
		}
	}
}

// 测试不同长度字符串的 loader 性能
func BenchmarkShardsValueString(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			value := generateString(size)

			// Simple 缓存
			b.Run("Simple", func(b *testing.B) {
				cache := buildBenchmarkShardsCache(1000)
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})

			// LRU 缓存
			b.Run("LRU", func(b *testing.B) {
				cache := New(1000).LRU().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})

			// ARC 缓存
			b.Run("ARC", func(b *testing.B) {
				cache := New(1000).ARC().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})

			// LFU 缓存
			b.Run("LFU", func(b *testing.B) {
				cache := New(1000).LFU().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})
		})
	}
}

// 测试不同长度字节数组的 loader 性能
func BenchmarkShardsValueBytes(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			value := generateBytes(size)

			// Simple 缓存
			b.Run("Simple", func(b *testing.B) {
				cache := buildBenchmarkShardsCache(1000)
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})

			// LRU 缓存
			b.Run("LRU", func(b *testing.B) {
				cache := New(1000).LRU().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})

			// ARC 缓存
			b.Run("ARC", func(b *testing.B) {
				cache := New(1000).ARC().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})

			// LFU 缓存
			b.Run("LFU", func(b *testing.B) {
				cache := New(1000).LFU().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})
		})
	}
}

// 测试不同长度结构体的 loader 性能
func BenchmarkShardsValueStruct(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			value := generateStruct(size)

			// Simple 缓存
			b.Run("Simple", func(b *testing.B) {
				cache := buildBenchmarkShardsCache(1000)
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})

			// LRU 缓存
			b.Run("LRU", func(b *testing.B) {
				cache := New(1000).LRU().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})

			// ARC 缓存
			b.Run("ARC", func(b *testing.B) {
				cache := New(1000).ARC().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})

			// LFU 缓存
			b.Run("LFU", func(b *testing.B) {
				cache := New(1000).LFU().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cache.Get(fmt.Sprintf("key_%d", i%1000))
				}
			})
		})
	}
}

// 测试不同长度 value 的并行 loader 性能
func BenchmarkShardsValueParallel(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			value := generateString(size)

			// Simple 缓存
			b.Run("Simple", func(b *testing.B) {
				cache := buildBenchmarkShardsCache(1000)
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					counter := 0
					for pb.Next() {
						_, _ = cache.Get(fmt.Sprintf("key_%d", counter%1000))
						counter++
					}
				})
			})

			// LRU 缓存
			b.Run("LRU", func(b *testing.B) {
				cache := New(1000).LRU().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					counter := 0
					for pb.Next() {
						_, _ = cache.Get(fmt.Sprintf("key_%d", counter%1000))
						counter++
					}
				})
			})

			// ARC 缓存
			b.Run("ARC", func(b *testing.B) {
				cache := New(1000).ARC().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					counter := 0
					for pb.Next() {
						_, _ = cache.Get(fmt.Sprintf("key_%d", counter%1000))
						counter++
					}
				})
			})

			// LFU 缓存
			b.Run("LFU", func(b *testing.B) {
				cache := New(1000).LFU().Build()
				loader := func(key interface{}) (interface{}, error) {
					return value, nil
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					counter := 0
					for pb.Next() {
						_, _ = cache.Get(fmt.Sprintf("key_%d", counter%1000))
						counter++
					}
				})
			})
		})
	}
}

// 测试混合不同长度 value 的 loader 性能
func BenchmarkShardsValueMixed(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			stringValue := generateString(size)
			bytesValue := generateBytes(size)
			structValue := generateStruct(size)

			// Simple 缓存
			b.Run("Simple", func(b *testing.B) {
				cache := buildBenchmarkShardsCache(1000)
				loader := func(key interface{}) (interface{}, error) {
					keyStr := key.(string)
					if strings.HasPrefix(keyStr, "str_") {
						return stringValue, nil
					} else if strings.HasPrefix(keyStr, "bytes_") {
						return bytesValue, nil
					} else {
						return structValue, nil
					}
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					keyType := i % 3
					var key string
					if keyType == 0 {
						key = fmt.Sprintf("str_%d", i%1000)
					} else if keyType == 1 {
						key = fmt.Sprintf("bytes_%d", i%1000)
					} else {
						key = fmt.Sprintf("struct_%d", i%1000)
					}
					_, _ = cache.Get(key)
				}
			})

			// LRU 缓存
			b.Run("LRU", func(b *testing.B) {
				cache := New(1000).LRU().Build()
				loader := func(key interface{}) (interface{}, error) {
					keyStr := key.(string)
					if strings.HasPrefix(keyStr, "str_") {
						return stringValue, nil
					} else if strings.HasPrefix(keyStr, "bytes_") {
						return bytesValue, nil
					} else {
						return structValue, nil
					}
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					keyType := i % 3
					var key string
					if keyType == 0 {
						key = fmt.Sprintf("str_%d", i%1000)
					} else if keyType == 1 {
						key = fmt.Sprintf("bytes_%d", i%1000)
					} else {
						key = fmt.Sprintf("struct_%d", i%1000)
					}
					_, _ = cache.Get(key)
				}
			})

			// ARC 缓存
			b.Run("ARC", func(b *testing.B) {
				cache := New(1000).ARC().Build()
				loader := func(key interface{}) (interface{}, error) {
					keyStr := key.(string)
					if strings.HasPrefix(keyStr, "str_") {
						return stringValue, nil
					} else if strings.HasPrefix(keyStr, "bytes_") {
						return bytesValue, nil
					} else {
						return structValue, nil
					}
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					keyType := i % 3
					var key string
					if keyType == 0 {
						key = fmt.Sprintf("str_%d", i%1000)
					} else if keyType == 1 {
						key = fmt.Sprintf("bytes_%d", i%1000)
					} else {
						key = fmt.Sprintf("struct_%d", i%1000)
					}
					_, _ = cache.Get(key)
				}
			})

			// LFU 缓存
			b.Run("LFU", func(b *testing.B) {
				cache := New(1000).LFU().Build()
				loader := func(key interface{}) (interface{}, error) {
					keyStr := key.(string)
					if strings.HasPrefix(keyStr, "str_") {
						return stringValue, nil
					} else if strings.HasPrefix(keyStr, "bytes_") {
						return bytesValue, nil
					} else {
						return structValue, nil
					}
				}
				cache = buildBenchmarkLoadingShardsCache(1000, loader)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					keyType := i % 3
					var key string
					if keyType == 0 {
						key = fmt.Sprintf("str_%d", i%1000)
					} else if keyType == 1 {
						key = fmt.Sprintf("bytes_%d", i%1000)
					} else {
						key = fmt.Sprintf("struct_%d", i%1000)
					}
					_, _ = cache.Get(key)
				}
			})
		})
	}
}

// 测试不同缓存类型的性能
func BenchmarkCacheTypes(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	cacheTypes := []struct {
		name  string
		build func(int) Cache
	}{
		{"Simple", buildBenchmarkShardsCache},
		{"LRU", buildBenchmarkLRUCache},
		{"LFU", buildBenchmarkLFUCache},
		{"ARC", buildBenchmarkARCCache},
	}

	for _, size := range sizes {
		for _, ct := range cacheTypes {
			// 测试写入性能
			b.Run(fmt.Sprintf("%s_Write_size_%d", ct.name, size), func(b *testing.B) {
				cache := ct.build(size)
				data := generateTestDataAndReset(b, b.N)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cache.Set(data[i].key, data[i].value)
				}
			})

			// 测试读取性能
			b.Run(fmt.Sprintf("%s_Read_size_%d", ct.name, size), func(b *testing.B) {
				cache := ct.build(size)
				data := generateTestDataAndReset(b, size)
				// 预填充缓存
				for i := 0; i < size; i++ {
					cache.Set(data[i].key, data[i].value)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cache.Get(data[i%size].key)
				}
			})

			// 测试并发写入性能
			b.Run(fmt.Sprintf("%s_ParallelWrite_size_%d", ct.name, size), func(b *testing.B) {
				cache := ct.build(size)
				data := generateTestDataAndReset(b, b.N)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						cache.Set(data[i].key, data[i].value)
						i = (i + 1) % b.N
					}
				})
			})

			// 测试并发读取性能
			b.Run(fmt.Sprintf("%s_ParallelRead_size_%d", ct.name, size), func(b *testing.B) {
				cache := ct.build(size)
				data := generateTestDataAndReset(b, size)
				// 预填充缓存
				for i := 0; i < size; i++ {
					cache.Set(data[i].key, data[i].value)
				}
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						cache.Get(data[i%size].key)
						i = (i + 1) % size
					}
				})
			})

			// 测试混合读写性能
			b.Run(fmt.Sprintf("%s_Mixed_size_%d", ct.name, size), func(b *testing.B) {
				cache := ct.build(size)
				data := generateTestDataAndReset(b, size)
				// 预填充缓存
				for i := 0; i < size/2; i++ {
					cache.Set(data[i].key, data[i].value)
				}
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						if i%2 == 0 {
							cache.Get(data[i%size].key)
						} else {
							cache.Set(data[i].key, data[i].value)
						}
						i = (i + 1) % size
					}
				})
			})
		}
	}
}
