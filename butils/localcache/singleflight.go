package localcache

import "sync"

// call 是一个正在进行的或完成的Do调用.
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group 表示一类工作，并形成一个命名空间，在其中可以执行具有重复抑制的单元工作.
type Group struct {
	cache Cache
	mu    sync.Mutex            // protects m
	m     map[interface{}]*call // lazily initialized
}

// Do 执行并返回给定函数的执行结果，确保在给定键的执行期间只有一个执行.
// 如果一个重复的调用进来，重复的调用者等待原始调用完成并接收相同的结果.
func (g *Group) Do(key interface{}, fn func() (interface{}, error), isWait bool) (interface{}, bool, error) {
	g.mu.Lock()
	v, err := g.cache.get(key, true)
	if err == nil {
		g.mu.Unlock()
		return v, false, nil
	}
	if g.m == nil {
		g.m = make(map[interface{}]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		if !isWait {
			return nil, false, KeyNotFoundError
		}
		c.wg.Wait()
		return c.val, false, c.err
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()
	if !isWait {
		go g.call(c, key, fn)
		return nil, false, KeyNotFoundError
	}
	v, err = g.call(c, key, fn)
	return v, true, err
}

func (g *Group) call(c *call, key interface{}, fn func() (interface{}, error)) (interface{}, error) {
	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
