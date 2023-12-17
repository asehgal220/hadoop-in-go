package distributedgrepserver

import (
	"sync"

	orderedmap "github.com/wk8/go-ordered-map"
)

type Cache struct {
	Cache    orderedmap.OrderedMap
	Capacity int64
	mu       sync.Mutex
}

// Constructs and returns a cache object.
func createCache(capacity int64) *Cache {
	cache := orderedmap.New()
	rv := new(Cache)

	rv.Cache = *cache
	rv.Capacity = capacity

	return rv
}

// Put a key value pair into the cache. Evicts the lru value until capacity is met.
func put(_cache *Cache, key string, val string) {
	_cache.mu.Lock()
	defer _cache.mu.Unlock()

	_cache.Cache.Set(key, val)

	for int64(_cache.Cache.Len()) > _cache.Capacity {
		lastKey := _cache.Cache.Oldest()
		_cache.Cache.Delete(lastKey)
	}
}

// Returns value for key from the cache. Returns nil if key value pair doesn't exist.
func get(_cache *Cache, key string) interface{} {
	_cache.mu.Lock()
	val, isPresent := _cache.Cache.Get(key)
	if isPresent {
		_cache.Cache.MoveToFront(key) // Move value to the front of the queue if already exists in cache
	}
	_cache.mu.Unlock()

	switch value := val.(type) {
	case string:
		return value
	default:
		return nil
	}
}
