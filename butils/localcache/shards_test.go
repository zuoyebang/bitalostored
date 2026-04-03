package localcache

import (
	"fmt"
	"testing"
	"time"
)

func TestShardsGet(t *testing.T) {
	size := 1000
	gc := buildTestShardsCache(t, size)
	testSetCache(t, gc, size-100)
	testGetCache(t, gc, size-100)
}

func TestShardsGTMaxSize(t *testing.T) {
	size := maxShards*shardSize + shardSize
	gc := buildTestShardsCache(t, size)
	testSetCache(t, gc, maxShards*shardSize-maxShards)
	testGetCache(t, gc, maxShards*shardSize-maxShards)
}

func TestLoadingShardsGet(t *testing.T) {
	size := 1000
	numbers := 1000
	testGetCache(t, buildTestLoadingShardsCache(t, size, loader), numbers)
}

func TestShardsLength(t *testing.T) {
	gc := buildTestLoadingShardsCache(t, 1000, loader)
	gc.Get("test1")
	gc.Get("test2")
	length := gc.Len(true)
	expectedLength := 2
	if length != expectedLength {
		t.Errorf("Expected length is %v, not %v", length, expectedLength)
	}
}

func TestShardsEvictItem(t *testing.T) {
	cacheSize := 10
	numbers := 11
	gc := buildTestLoadingShardsCache(t, cacheSize, loader)

	for i := 0; i < numbers; i++ {
		_, err := gc.Get(fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestShardsUnboundedNoEviction(t *testing.T) {
	numbers := 1000
	size_tracker := 0
	gcu := buildTestLoadingShardsCache(t, 0, loader)

	for i := 0; i < numbers; i++ {
		current_size := gcu.Len(true)
		if current_size != size_tracker {
			t.Errorf("Excepted cache size is %v not %v", current_size, size_tracker)
		}

		_, err := gcu.Get(fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		size_tracker++
	}
}

func TestShardsGetIFPresent(t *testing.T) {
	gc := buildTestLoadingShardsCache(t, 1000, loader)

	// 测试不存在的键
	_, err := gc.GetIFPresent("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent key")
	}

	// 测试存在的键
	gc.Set("test", "value")
	val, err := gc.GetIFPresent("test")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != "value" {
		t.Errorf("Expected value 'value', got %v", val)
	}
}

func TestShardsHas(t *testing.T) {
	gc := buildTestLoadingShardsCacheWithExpiration(t, 4, 10*time.Millisecond)

	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			gc.Get("test1")
			gc.Get("test2")

			if gc.Has("test0") {
				t.Fatal("should not have test0")
			}
			if !gc.Has("test1") {
				t.Fatal("should have test1")
			}
			if !gc.Has("test2") {
				t.Fatal("should have test2")
			}

			time.Sleep(20 * time.Millisecond)

			if gc.Has("test0") {
				t.Fatal("should not have test0")
			}
			if gc.Has("test1") {
				t.Fatal("should not have test1")
			}
			if gc.Has("test2") {
				t.Fatal("should not have test2")
			}
		})
	}
}

// 辅助函数
func buildTestShardsCache(t *testing.T, size int) Cache {
	return New(size).
		Simple().Build()
}

func buildTestLoadingShardsCache(t *testing.T, size int, loader LoaderFunc) Cache {
	return New(size).
		LoaderFunc(loader).
		Build()
}

func buildTestLoadingShardsCacheWithExpiration(t *testing.T, size int, expiration time.Duration) Cache {
	return New(size).
		LoaderFunc(loader).
		Expiration(expiration).
		Build()
}
