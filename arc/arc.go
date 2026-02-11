package arc

import (
	"sync"
	"sync/atomic"
)

// CacheStats holds runtime statistics for the ARC cache.
type CacheStats struct {
	Hits         int64 // number of successful Get calls
	Misses       int64 // number of Get calls that returned no value
	Entries      int   // current number of cached entries (T1 + T2)
	CurrentBytes int64 // current estimated memory usage in bytes
	T1Len        int   // entries in T1 (recently accessed once)
	T2Len        int   // entries in T2 (accessed at least twice)
	B1Len        int   // ghost entries in B1
	B2Len        int   // ghost entries in B2
	P            int   // current adaptive parameter
}

// Option configures an ARCCache via the functional options pattern.
type Option[K comparable, V any] func(*ARCCache[K, V])

// WithMaxBytes sets the maximum memory limit in bytes. If 0 or negative,
// only the entry count limit applies.
func WithMaxBytes[K comparable, V any](maxBytes int64) Option[K, V] {
	return func(c *ARCCache[K, V]) {
		c.maxBytes = maxBytes
	}
}

// WithOnEvict registers a callback that is invoked whenever an entry is
// evicted from the cache (moved to a ghost list or removed entirely).
// The callback receives the key and value of the evicted entry.
// Note: the callback is called while the cache lock is held — keep it fast.
func WithOnEvict[K comparable, V any](fn func(K, V)) Option[K, V] {
	return func(c *ARCCache[K, V]) {
		c.onEvict = fn
	}
}

// WithSizeFunc registers a function to automatically compute the memory
// size of entries. When set, Put can be called with size=0 and the cache
// will compute the size via this function. If not set, the caller must
// provide an accurate size in each Put call.
func WithSizeFunc[K comparable, V any](fn func(K, V) int64) Option[K, V] {
	return func(c *ARCCache[K, V]) {
		c.sizeFunc = fn
	}
}

// ARCCache implements the Adaptive Replacement Cache algorithm.
//
// It maintains four lists:
//   - T1: pages seen exactly once recently (recency)
//   - T2: pages seen at least twice recently (frequency)
//   - B1: ghost entries recently evicted from T1
//   - B2: ghost entries recently evicted from T2
//
// The parameter p dynamically adapts to shift the balance between
// recency and frequency based on workload patterns.
//
// Capacity is bounded by both maxEntries and maxBytes (whichever is hit first).
type ARCCache[K comparable, V any] struct {
	maxEntries int
	maxBytes   int64

	mu           sync.Mutex
	p            int   // target size for T1; 0 <= p <= maxEntries
	currentBytes int64 // sum of all entry sizes in T1 + T2

	t1 doublyLinkedList[K, V] // recent, seen once
	t2 doublyLinkedList[K, V] // frequent, seen >= 2 times
	b1 doublyLinkedList[K, V] // ghost list for T1 evictions
	b2 doublyLinkedList[K, V] // ghost list for T2 evictions

	items   map[K]*entry[K, V] // key → entry in T1 or T2
	ghostB1 map[K]*entry[K, V] // key → ghost entry in B1
	ghostB2 map[K]*entry[K, V] // key → ghost entry in B2

	pool entryPool[K, V] // recycled entry objects to reduce GC pressure

	onEvict  func(K, V)       // optional eviction callback
	sizeFunc func(K, V) int64 // optional auto-size function

	hits   int64 // updated under mu — no atomic needed
	misses int64 // updated under mu — no atomic needed

	// loadGroup deduplicates concurrent GetOrLoad calls for the same key.
	loadGroup singleflightGroup[K, V]
}

// singleflightGroup provides call deduplication for GetOrLoad.
type singleflightGroup[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*call[V]
}

type call[V any] struct {
	wg  sync.WaitGroup
	val V
	err error
}

func (g *singleflightGroup[K, V]) Do(key K, fn func() (V, error)) (V, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*call[V])
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &call[V]{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}

// entryPool recycles entry objects to reduce heap allocations.
type entryPool[K comparable, V any] struct {
	pool []*entry[K, V]
}

func (p *entryPool[K, V]) get() *entry[K, V] {
	if len(p.pool) == 0 {
		return &entry[K, V]{}
	}
	e := p.pool[len(p.pool)-1]
	p.pool = p.pool[:len(p.pool)-1]
	return e
}

func (p *entryPool[K, V]) put(e *entry[K, V]) {
	// Cap pool size to avoid unbounded growth.
	if len(p.pool) >= 256 {
		return
	}
	var zeroK K
	var zeroV V
	e.key = zeroK
	e.value = zeroV
	e.size = 0
	e.prev = nil
	e.next = nil
	e.ghost = false
	e.list = listNone
	p.pool = append(p.pool, e)
}

// NewARCCache creates a new ARC cache bounded by maxEntries.
// Use options to configure maxBytes, eviction callbacks, and auto-sizing.
// maxEntries must be > 0.
func NewARCCache[K comparable, V any](maxEntries int, maxBytes int64, opts ...Option[K, V]) *ARCCache[K, V] {
	if maxEntries <= 0 {
		maxEntries = 1
	}
	c := &ARCCache[K, V]{
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		items:      make(map[K]*entry[K, V]),
		ghostB1:    make(map[K]*entry[K, V]),
		ghostB2:    make(map[K]*entry[K, V]),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Get retrieves the value associated with key.
// If the key is in T1, it is promoted to T2 (frequency promotion).
// If the key is in T2, it is moved to the front of T2.
// Returns (value, true) on hit, (zero, false) on miss.
func (c *ARCCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()

	if e, ok := c.items[key]; ok {
		c.promoteToT2(e)
		c.hits++
		val := e.value
		c.mu.Unlock()
		return val, true
	}

	c.misses++
	c.mu.Unlock()
	var zero V
	return zero, false
}

// Peek retrieves the value associated with key WITHOUT promoting it.
// This is useful for inspecting cache contents without affecting the
// ARC recency/frequency ordering.
func (c *ARCCache[K, V]) Peek(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.items[key]; ok {
		return e.value, true
	}
	var zero V
	return zero, false
}

// GetOrLoad retrieves the value for key, or calls loader to compute it
// if not cached. Only one loader call per key happens concurrently
// (singleflight deduplication), preventing thundering-herd on cache miss.
//
// The loader function receives the key and must return the value and
// its estimated memory size. If loader returns an error, the value is
// not cached and the error is returned to all waiters.
func (c *ARCCache[K, V]) GetOrLoad(key K, loader func(K) (V, int64, error)) (V, error) {
	// Fast path: check cache.
	if val, ok := c.Get(key); ok {
		return val, nil
	}

	// Slow path: deduplicated load.
	type result struct {
		val  V
		size int64
	}
	val, err := c.loadGroup.Do(key, func() (V, error) {
		// Double-check after winning the singleflight race.
		if val, ok := c.Get(key); ok {
			return val, nil
		}

		v, size, err := loader(key)
		if err != nil {
			var zero V
			return zero, err
		}

		c.Put(key, v, size)
		return v, nil
	})

	return val, err
}

// Put inserts or updates a key-value pair.
// If a SizeFunc was configured and size is 0, the size is computed automatically.
// The ARC algorithm adapts parameter p based on ghost-list hits.
func (c *ARCCache[K, V]) Put(key K, value V, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Auto-compute size if a SizeFunc is set and caller passed 0.
	if size == 0 && c.sizeFunc != nil {
		size = c.sizeFunc(key, value)
	}

	// Case 1: key already in cache (T1 or T2) — update value and promote.
	if e, ok := c.items[key]; ok {
		c.currentBytes += size - e.size
		e.value = value
		e.size = size
		c.promoteToT2(e)
		c.enforceMemoryLimit()
		return
	}

	// Case 2: key found in ghost list B1 — adapt toward recency.
	if ge, ok := c.ghostB1[key]; ok {
		delta := 1
		if c.b2.Len() > c.b1.Len() {
			delta = c.b2.Len() / c.b1.Len()
		}
		c.p = min(c.p+delta, c.maxEntries)

		c.replace(key)

		c.b1.Remove(ge)
		delete(c.ghostB1, key)

		ge.value = value
		ge.size = size
		ge.ghost = false
		ge.list = listT2
		c.t2.PushFront(ge)
		c.items[key] = ge
		c.currentBytes += size

		c.enforceMemoryLimit()
		return
	}

	// Case 3: key found in ghost list B2 — adapt toward frequency.
	if ge, ok := c.ghostB2[key]; ok {
		delta := 1
		if c.b1.Len() > c.b2.Len() {
			delta = c.b1.Len() / c.b2.Len()
		}
		c.p = max(c.p-delta, 0)

		c.replace(key)

		c.b2.Remove(ge)
		delete(c.ghostB2, key)

		ge.value = value
		ge.size = size
		ge.ghost = false
		ge.list = listT2
		c.t2.PushFront(ge)
		c.items[key] = ge
		c.currentBytes += size

		c.enforceMemoryLimit()
		return
	}

	// Case 4: complete miss — key not in T1, T2, B1, or B2.

	// Sub-case A: T1 + B1 directory is full.
	if c.t1.Len()+c.b1.Len() >= c.maxEntries {
		if c.t1.Len() < c.maxEntries {
			evicted := c.b1.RemoveBack()
			if evicted != nil {
				delete(c.ghostB1, evicted.key)
				c.pool.put(evicted)
			}
			c.replace(key)
		} else {
			evicted := c.t1.RemoveBack()
			if evicted != nil {
				c.currentBytes -= evicted.size
				delete(c.items, evicted.key)
				c.notifyEvict(evicted)
				c.pool.put(evicted)
			}
		}
	} else {
		total := c.t1.Len() + c.t2.Len() + c.b1.Len() + c.b2.Len()
		if total >= c.maxEntries*2 {
			evicted := c.b2.RemoveBack()
			if evicted != nil {
				delete(c.ghostB2, evicted.key)
				c.pool.put(evicted)
			}
		}
		if c.t1.Len()+c.t2.Len() >= c.maxEntries {
			c.replace(key)
		}
	}

	// Insert new entry into T1 (from pool if available).
	e := c.pool.get()
	e.key = key
	e.value = value
	e.size = size
	e.list = listT1
	e.ghost = false
	c.t1.PushFront(e)
	c.items[key] = e
	c.currentBytes += size

	c.enforceMemoryLimit()
}

// Delete removes a key from the cache entirely (from T1, T2, B1, or B2).
func (c *ARCCache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.items[key]; ok {
		c.removeFromMainLists(e)
		c.pool.put(e)
		return
	}
	if ge, ok := c.ghostB1[key]; ok {
		c.b1.Remove(ge)
		delete(c.ghostB1, key)
		c.pool.put(ge)
		return
	}
	if ge, ok := c.ghostB2[key]; ok {
		c.b2.Remove(ge)
		delete(c.ghostB2, key)
		c.pool.put(ge)
	}
}

// Len returns the number of entries currently in the cache (T1 + T2).
func (c *ARCCache[K, V]) Len() int {
	c.mu.Lock()
	n := c.t1.Len() + c.t2.Len()
	c.mu.Unlock()
	return n
}

// Keys returns a snapshot of all keys currently in the cache.
// The order is not guaranteed.
func (c *ARCCache[K, V]) Keys() []K {
	c.mu.Lock()
	defer c.mu.Unlock()

	keys := make([]K, 0, len(c.items))
	for k := range c.items {
		keys = append(keys, k)
	}
	return keys
}

// Contains checks if a key exists in the cache without promoting it.
func (c *ARCCache[K, V]) Contains(key K) bool {
	c.mu.Lock()
	_, ok := c.items[key]
	c.mu.Unlock()
	return ok
}

// Clear resets the cache to its initial empty state.
func (c *ARCCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t1.Clear()
	c.t2.Clear()
	c.b1.Clear()
	c.b2.Clear()
	c.items = make(map[K]*entry[K, V])
	c.ghostB1 = make(map[K]*entry[K, V])
	c.ghostB2 = make(map[K]*entry[K, V])
	c.p = 0
	c.currentBytes = 0
	c.pool.pool = c.pool.pool[:0]
}

// Stats returns a snapshot of the cache statistics.
func (c *ARCCache[K, V]) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	return CacheStats{
		Hits:         atomic.LoadInt64(&c.hits),
		Misses:       atomic.LoadInt64(&c.misses),
		Entries:      c.t1.Len() + c.t2.Len(),
		CurrentBytes: c.currentBytes,
		T1Len:        c.t1.Len(),
		T2Len:        c.t2.Len(),
		B1Len:        c.b1.Len(),
		B2Len:        c.b2.Len(),
		P:            c.p,
	}
}

// --- internal helpers (caller must hold c.mu) ---

// notifyEvict calls the onEvict callback if set.
func (c *ARCCache[K, V]) notifyEvict(e *entry[K, V]) {
	if c.onEvict != nil && !e.ghost {
		c.onEvict(e.key, e.value)
	}
}

// promoteToT2 moves an entry from T1 to the front of T2,
// or moves it to the front of T2 if it's already there.
func (c *ARCCache[K, V]) promoteToT2(e *entry[K, V]) {
	if e.list == listT1 {
		c.t1.Remove(e)
		c.t2.PushFront(e)
		e.list = listT2
	} else {
		c.t2.MoveToFront(e)
	}
}

// replace is the core ARC replacement subroutine.
func (c *ARCCache[K, V]) replace(key K) {
	if c.t1.Len() == 0 && c.t2.Len() == 0 {
		return
	}

	_, inB2 := c.ghostB2[key]

	if c.t1.Len() > 0 && (c.t1.Len() > c.p || (inB2 && c.t1.Len() == c.p)) {
		evicted := c.t1.RemoveBack()
		if evicted != nil {
			c.evictToGhost(evicted, &c.b1, c.ghostB1, listB1)
		}
	} else if c.t2.Len() > 0 {
		evicted := c.t2.RemoveBack()
		if evicted != nil {
			c.evictToGhost(evicted, &c.b2, c.ghostB2, listB2)
		}
	} else if c.t1.Len() > 0 {
		evicted := c.t1.RemoveBack()
		if evicted != nil {
			c.evictToGhost(evicted, &c.b1, c.ghostB1, listB1)
		}
	}
}

// removeFromMainLists removes an entry from T1 or T2 and its items map.
func (c *ARCCache[K, V]) removeFromMainLists(e *entry[K, V]) {
	if e.list == listT1 {
		c.t1.Remove(e)
	} else {
		c.t2.Remove(e)
	}
	e.list = listNone
	c.currentBytes -= e.size
	delete(c.items, e.key)
}

// evictToGhost moves an evicted entry to the specified ghost list,
// clearing its value to free memory.
func (c *ARCCache[K, V]) evictToGhost(
	e *entry[K, V],
	ghostList *doublyLinkedList[K, V],
	ghostMap map[K]*entry[K, V],
	targetList listID,
) {
	c.notifyEvict(e)
	delete(c.items, e.key)
	c.currentBytes -= e.size
	var zero V
	e.value = zero
	e.size = 0
	e.ghost = true
	e.list = targetList
	ghostList.PushFront(e)
	ghostMap[e.key] = e
}

// trimGhosts ensures the total directory size (T1+T2+B1+B2) does not
// exceed 2*maxEntries by evicting the oldest ghost entries.
func (c *ARCCache[K, V]) trimGhosts() {
	for c.b1.Len()+c.b2.Len()+c.t1.Len()+c.t2.Len() > c.maxEntries*2 {
		if c.b1.Len() >= c.b2.Len() && c.b1.Len() > 0 {
			if evicted := c.b1.RemoveBack(); evicted != nil {
				delete(c.ghostB1, evicted.key)
				c.pool.put(evicted)
			}
		} else if c.b2.Len() > 0 {
			if evicted := c.b2.RemoveBack(); evicted != nil {
				delete(c.ghostB2, evicted.key)
				c.pool.put(evicted)
			}
		} else {
			break
		}
	}
}

// enforceMemoryLimit evicts entries while currentBytes exceeds maxBytes.
func (c *ARCCache[K, V]) enforceMemoryLimit() {
	if c.maxBytes <= 0 {
		return
	}

	for c.currentBytes > c.maxBytes && (c.t1.Len()+c.t2.Len()) > 0 {
		if c.t1.Len() > c.p && c.t1.Len() > 0 {
			evicted := c.t1.RemoveBack()
			if evicted != nil {
				c.evictToGhost(evicted, &c.b1, c.ghostB1, listB1)
			}
		} else if c.t2.Len() > 0 {
			evicted := c.t2.RemoveBack()
			if evicted != nil {
				c.evictToGhost(evicted, &c.b2, c.ghostB2, listB2)
			}
		} else if c.t1.Len() > 0 {
			evicted := c.t1.RemoveBack()
			if evicted != nil {
				c.evictToGhost(evicted, &c.b1, c.ghostB1, listB1)
			}
		} else {
			break
		}
	}

	c.trimGhosts()
}
