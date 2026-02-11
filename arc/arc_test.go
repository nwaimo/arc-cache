package arc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// --- doubly-linked list tests ---

func TestList_PushFrontAndRemoveBack(t *testing.T) {
	var l doublyLinkedList[int, string]

	e1 := &entry[int, string]{key: 1, value: "a"}
	e2 := &entry[int, string]{key: 2, value: "b"}
	e3 := &entry[int, string]{key: 3, value: "c"}

	l.PushFront(e1)
	l.PushFront(e2)
	l.PushFront(e3)

	if l.Len() != 3 {
		t.Fatalf("expected len 3, got %d", l.Len())
	}

	if back := l.Back(); back != e1 {
		t.Fatalf("expected back key=1, got key=%d", back.key)
	}

	if front := l.Front(); front != e3 {
		t.Fatalf("expected front key=3, got key=%d", front.key)
	}

	removed := l.RemoveBack()
	if removed.key != 1 {
		t.Fatalf("expected removed key=1, got %d", removed.key)
	}
	if l.Len() != 2 {
		t.Fatalf("expected len 2, got %d", l.Len())
	}
}

func TestList_MoveToFront(t *testing.T) {
	var l doublyLinkedList[int, string]

	e1 := &entry[int, string]{key: 1, value: "a"}
	e2 := &entry[int, string]{key: 2, value: "b"}
	e3 := &entry[int, string]{key: 3, value: "c"}

	l.PushFront(e1)
	l.PushFront(e2)
	l.PushFront(e3)

	l.MoveToFront(e1)

	if l.Front() != e1 {
		t.Fatalf("expected front key=1 after MoveToFront, got key=%d", l.Front().key)
	}
	if l.Len() != 3 {
		t.Fatalf("expected len 3, got %d", l.Len())
	}
}

func TestList_Remove(t *testing.T) {
	var l doublyLinkedList[int, string]

	e1 := &entry[int, string]{key: 1, value: "a"}
	e2 := &entry[int, string]{key: 2, value: "b"}
	e3 := &entry[int, string]{key: 3, value: "c"}

	l.PushFront(e1)
	l.PushFront(e2)
	l.PushFront(e3)

	l.Remove(e2)

	if l.Len() != 2 {
		t.Fatalf("expected len 2, got %d", l.Len())
	}
	if l.Front() != e3 || l.Back() != e1 {
		t.Fatal("incorrect list structure after removing middle element")
	}
}

func TestList_Clear(t *testing.T) {
	var l doublyLinkedList[int, string]

	l.PushFront(&entry[int, string]{key: 1, value: "a"})
	l.PushFront(&entry[int, string]{key: 2, value: "b"})
	l.Clear()

	if l.Len() != 0 {
		t.Fatalf("expected len 0 after Clear, got %d", l.Len())
	}
	if l.Front() != nil || l.Back() != nil {
		t.Fatal("expected nil head/tail after Clear")
	}
}

func TestList_RemoveBack_Empty(t *testing.T) {
	var l doublyLinkedList[int, string]

	if e := l.RemoveBack(); e != nil {
		t.Fatalf("expected nil from RemoveBack on empty list, got %v", e)
	}
}

// --- ARC cache tests ---

func TestARC_BasicGetPut(t *testing.T) {
	c := NewARCCache[string, int](4, 0)

	c.Put("a", 1, 8)
	c.Put("b", 2, 8)
	c.Put("c", 3, 8)
	c.Put("d", 4, 8)

	for _, tc := range []struct {
		key string
		val int
	}{
		{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4},
	} {
		val, ok := c.Get(tc.key)
		if !ok {
			t.Errorf("expected key %q to be found", tc.key)
		}
		if val != tc.val {
			t.Errorf("key %q: expected %d, got %d", tc.key, tc.val, val)
		}
	}

	stats := c.Stats()
	if stats.Hits != 4 || stats.Misses != 0 {
		t.Errorf("expected 4 hits / 0 misses, got %d / %d", stats.Hits, stats.Misses)
	}
}

func TestARC_Miss(t *testing.T) {
	c := NewARCCache[string, int](4, 0)

	_, ok := c.Get("nonexistent")
	if ok {
		t.Error("expected miss for nonexistent key")
	}

	stats := c.Stats()
	if stats.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.Misses)
	}
}

func TestARC_EvictionByEntryCount(t *testing.T) {
	c := NewARCCache[string, int](3, 0)

	c.Put("a", 1, 8)
	c.Put("b", 2, 8)
	c.Put("c", 3, 8)

	if _, ok := c.Get("a"); !ok {
		t.Error("expected 'a' to be present")
	}

	c.Put("d", 4, 8)

	stats := c.Stats()
	if stats.Entries > 3 {
		t.Errorf("expected at most 3 entries, got %d", stats.Entries)
	}
}

func TestARC_T1ToT2Promotion(t *testing.T) {
	c := NewARCCache[string, int](4, 0)

	c.Put("a", 1, 8)

	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected 'a' to be found")
	}

	stats := c.Stats()
	if stats.T1Len != 0 || stats.T2Len != 1 {
		t.Errorf("expected T1=0, T2=1 after promotion; got T1=%d, T2=%d",
			stats.T1Len, stats.T2Len)
	}
}

func TestARC_GhostB1Hit_IncreaseP(t *testing.T) {
	c := NewARCCache[string, int](3, 0)

	c.Put("a", 1, 8) // T1: [a]
	c.Get("a")       // T2: [a], T1: []
	c.Put("b", 2, 8) // T1: [b], T2: [a]
	c.Put("c", 3, 8) // T1: [c, b], T2: [a]
	c.Put("d", 4, 8) // Cache full → replace evicts LRU of T1 ("b") to B1

	stats := c.Stats()
	if stats.B1Len != 1 {
		t.Fatalf("expected B1 len 1, got %d", stats.B1Len)
	}
	initialP := stats.P

	c.Put("b", 20, 8)

	stats = c.Stats()
	if stats.P <= initialP {
		t.Errorf("expected p to increase on B1 ghost hit; was %d, now %d",
			initialP, stats.P)
	}

	if val, ok := c.Get("b"); !ok || val != 20 {
		t.Errorf("expected 'b' = 20 in T2, got %v (found=%v)", val, ok)
	}
}

func TestARC_GhostB2Hit_DecreaseP(t *testing.T) {
	c := NewARCCache[string, int](2, 0)

	c.Put("a", 1, 8)
	c.Get("a") // promote to T2

	c.Put("b", 2, 8)
	c.Put("c", 3, 8) // should evict 'a' from T2 to B2

	stats := c.Stats()
	initialP := stats.P

	c.Put("a", 10, 8)

	stats = c.Stats()
	if stats.P > initialP {
		t.Errorf("expected p to decrease (or stay 0) on B2 ghost hit; was %d, now %d",
			initialP, stats.P)
	}
}

func TestARC_UpdateExistingKey(t *testing.T) {
	c := NewARCCache[string, int](4, 0)

	c.Put("a", 1, 8)
	c.Put("a", 99, 16)

	val, ok := c.Get("a")
	if !ok || val != 99 {
		t.Errorf("expected 'a' = 99, got %d (found=%v)", val, ok)
	}

	stats := c.Stats()
	if stats.Entries != 1 {
		t.Errorf("expected 1 entry after update, got %d", stats.Entries)
	}
}

func TestARC_Delete(t *testing.T) {
	c := NewARCCache[string, int](4, 0)

	c.Put("a", 1, 8)
	c.Put("b", 2, 8)

	c.Delete("a")

	if _, ok := c.Get("a"); ok {
		t.Error("expected 'a' to be deleted")
	}

	stats := c.Stats()
	if stats.Entries != 1 {
		t.Errorf("expected 1 entry after delete, got %d", stats.Entries)
	}
}

func TestARC_Clear(t *testing.T) {
	c := NewARCCache[string, int](4, 0)

	c.Put("a", 1, 8)
	c.Put("b", 2, 8)
	c.Put("c", 3, 8)

	c.Clear()

	stats := c.Stats()
	if stats.Entries != 0 {
		t.Errorf("expected 0 entries after Clear, got %d", stats.Entries)
	}
	if stats.CurrentBytes != 0 {
		t.Errorf("expected 0 bytes after Clear, got %d", stats.CurrentBytes)
	}
}

func TestARC_MemoryLimit(t *testing.T) {
	c := NewARCCache[string, int](100, 24)

	c.Put("a", 1, 8)
	c.Put("b", 2, 8)
	c.Put("c", 3, 8)

	stats := c.Stats()
	if stats.CurrentBytes > 24 {
		t.Errorf("expected <= 24 bytes, got %d", stats.CurrentBytes)
	}

	c.Put("d", 4, 8)

	stats = c.Stats()
	if stats.CurrentBytes > 24 {
		t.Errorf("expected <= 24 bytes after eviction, got %d", stats.CurrentBytes)
	}
	if stats.Entries > 3 {
		t.Errorf("expected <= 3 entries under memory limit, got %d", stats.Entries)
	}
}

func TestARC_ConcurrentAccess(t *testing.T) {
	c := NewARCCache[string, int](100, 0)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				c.Put(key, j, 8)
			}
		}(i)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				c.Get(key)
			}
		}(i)
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				c.Delete(key)
			}
		}(i)
	}

	wg.Wait()

	stats := c.Stats()
	if stats.Entries < 0 {
		t.Error("negative entry count")
	}
}

func TestARC_EvictionOrder(t *testing.T) {
	c := NewARCCache[string, string](3, 0)

	c.Put("a", "A", 8)
	c.Put("b", "B", 8)
	c.Put("c", "C", 8)

	c.Get("a")

	c.Put("d", "D", 8)

	if _, ok := c.Get("b"); ok {
		t.Error("expected 'b' to be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("expected 'a' to survive (in T2)")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("expected 'c' to survive")
	}
	if _, ok := c.Get("d"); !ok {
		t.Error("expected 'd' to survive")
	}
}

func TestARC_ZeroCapacity(t *testing.T) {
	c := NewARCCache[string, int](0, 0)

	c.Put("a", 1, 8)
	if val, ok := c.Get("a"); !ok || val != 1 {
		t.Error("expected cache to work with min capacity")
	}
}

// --- size estimation tests ---

func TestEstimateSize_Nil(t *testing.T) {
	if size := EstimateSize(nil); size != 0 {
		t.Errorf("expected 0 for nil, got %d", size)
	}
}

func TestEstimateSize_NonEmpty(t *testing.T) {
	result := &QueryResult{
		Columns: []string{"id", "name", "email"},
		Rows: [][]any{
			{1, "Alice", "alice@example.com"},
			{2, "Bob", "bob@example.com"},
		},
	}

	size := EstimateSize(result)
	if size <= 0 {
		t.Error("expected positive size for non-empty result")
	}
	if size < queryResultBaseOverhead {
		t.Errorf("expected size >= %d, got %d", queryResultBaseOverhead, size)
	}
}

// --- cache key tests ---

func TestCacheKey_Deterministic(t *testing.T) {
	k1 := cacheKey("SELECT * FROM users WHERE id = ?", 42)
	k2 := cacheKey("SELECT * FROM users WHERE id = ?", 42)

	if k1 != k2 {
		t.Error("expected identical cache keys for same query+args")
	}
}

func TestCacheKey_DifferentArgs(t *testing.T) {
	k1 := cacheKey("SELECT * FROM users WHERE id = ?", 1)
	k2 := cacheKey("SELECT * FROM users WHERE id = ?", 2)

	if k1 == k2 {
		t.Error("expected different cache keys for different args")
	}
}

func TestCacheKey_DifferentQueries(t *testing.T) {
	k1 := cacheKey("SELECT * FROM users", nil)
	k2 := cacheKey("SELECT * FROM orders", nil)

	if k1 == k2 {
		t.Error("expected different cache keys for different queries")
	}
}

func TestCacheKey_TypeCollision(t *testing.T) {
	k1 := cacheKey("SELECT * FROM users WHERE id = ?", 1)
	k2 := cacheKey("SELECT * FROM users WHERE id = ?", "1")

	if k1 == k2 {
		t.Error("expected different cache keys for int(1) vs string(\"1\")")
	}

	k3 := cacheKey("SELECT * FROM users WHERE active = ?", true)
	k4 := cacheKey("SELECT * FROM users WHERE active = ?", "true")

	if k3 == k4 {
		t.Error("expected different cache keys for bool(true) vs string(\"true\")")
	}

	k5 := cacheKey("SELECT * FROM users WHERE id = ?", int(1))
	k6 := cacheKey("SELECT * FROM users WHERE id = ?", int64(1))

	if k5 == k6 {
		t.Error("expected different cache keys for int(1) vs int64(1)")
	}
}

func TestARC_GhostBound_UnderMemoryPressure(t *testing.T) {
	c := NewARCCache[int, string](1000, 32)

	for i := 0; i < 500; i++ {
		c.Put(i, fmt.Sprintf("value-%d", i), 16)
	}

	stats := c.Stats()
	totalDirectory := stats.T1Len + stats.T2Len + stats.B1Len + stats.B2Len
	maxDirectory := 1000 * 2

	if totalDirectory > maxDirectory {
		t.Errorf("ghost lists exceeded 2*maxEntries bound: total=%d, max=%d",
			totalDirectory, maxDirectory)
	}
}

// --- isWriteQuery tests (including CTE + procedure support) ---

func TestIsWriteQuery(t *testing.T) {
	writes := []string{
		"INSERT INTO users (name) VALUES ('Alice')",
		"  UPDATE users SET name = 'Bob' WHERE id = 1",
		"\n\tDELETE FROM users WHERE id = 1",
		"DROP TABLE users",
		"ALTER TABLE users ADD COLUMN age INT",
		"TRUNCATE TABLE users",
		"CREATE TABLE test (id INT)",
		"insert into users values (1)",
		"CALL my_procedure(1, 2)",
		"GRANT SELECT ON users TO bob",
		"REVOKE SELECT ON users FROM bob",
		// CTE wrapping a write
		"WITH t AS (SELECT 1) INSERT INTO users (name) SELECT * FROM t",
		"WITH t AS (SELECT 1) DELETE FROM users WHERE id IN (SELECT * FROM t)",
		"WITH RECURSIVE t AS (SELECT 1 UNION ALL SELECT 1) UPDATE users SET name='x'",
	}

	for _, q := range writes {
		if !isWriteQuery(q) {
			t.Errorf("expected %q to be detected as write query", q)
		}
	}

	reads := []string{
		"SELECT * FROM users",
		"  select * from users",
		"WITH cte AS (SELECT 1) SELECT * FROM cte",
		"SHOW TABLES",
		"DESCRIBE users",
		"EXPLAIN SELECT * FROM users",
	}

	for _, q := range reads {
		if isWriteQuery(q) {
			t.Errorf("expected %q to NOT be detected as write query", q)
		}
	}
}

// --- Peek tests ---

func TestARC_Peek(t *testing.T) {
	c := NewARCCache[string, int](4, 0)

	c.Put("a", 1, 8)

	// Peek should find the value but NOT promote to T2.
	val, ok := c.Peek("a")
	if !ok || val != 1 {
		t.Errorf("Peek: expected (1, true), got (%d, %v)", val, ok)
	}

	stats := c.Stats()
	if stats.T1Len != 1 || stats.T2Len != 0 {
		t.Errorf("Peek should not promote; T1=%d, T2=%d", stats.T1Len, stats.T2Len)
	}

	// Peek miss.
	_, ok = c.Peek("nonexistent")
	if ok {
		t.Error("Peek should return false for missing key")
	}
}

// --- OnEvict callback tests ---

func TestARC_OnEvict(t *testing.T) {
	var evictedKeys []string
	c := NewARCCache[string, int](2, 0,
		WithOnEvict[string, int](func(k string, _ int) {
			evictedKeys = append(evictedKeys, k)
		}),
	)

	c.Put("a", 1, 8)
	c.Put("b", 2, 8)
	c.Put("c", 3, 8) // should evict one entry

	if len(evictedKeys) == 0 {
		t.Fatal("expected at least one eviction callback")
	}
}

// --- Len / Keys / Contains tests ---

func TestARC_Len(t *testing.T) {
	c := NewARCCache[string, int](10, 0)

	if c.Len() != 0 {
		t.Errorf("expected Len=0 on empty cache, got %d", c.Len())
	}

	c.Put("a", 1, 8)
	c.Put("b", 2, 8)

	if c.Len() != 2 {
		t.Errorf("expected Len=2, got %d", c.Len())
	}

	c.Delete("a")
	if c.Len() != 1 {
		t.Errorf("expected Len=1 after delete, got %d", c.Len())
	}
}

func TestARC_Keys(t *testing.T) {
	c := NewARCCache[string, int](10, 0)

	c.Put("a", 1, 8)
	c.Put("b", 2, 8)
	c.Put("c", 3, 8)

	keys := c.Keys()
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}

	found := map[string]bool{}
	for _, k := range keys {
		found[k] = true
	}
	for _, expected := range []string{"a", "b", "c"} {
		if !found[expected] {
			t.Errorf("expected key %q in Keys()", expected)
		}
	}
}

func TestARC_Contains(t *testing.T) {
	c := NewARCCache[string, int](10, 0)

	c.Put("a", 1, 8)

	if !c.Contains("a") {
		t.Error("expected Contains(a) = true")
	}
	if c.Contains("b") {
		t.Error("expected Contains(b) = false")
	}
}

// --- SizeFunc tests ---

func TestARC_SizeFunc(t *testing.T) {
	c := NewARCCache[string, string](100, 100,
		WithSizeFunc[string, string](func(_ string, v string) int64 {
			return int64(len(v))
		}),
	)

	// Pass size=0 — should auto-compute.
	c.Put("a", "hello", 0)      // 5 bytes
	c.Put("b", "world!!!!!", 0) // 10 bytes

	stats := c.Stats()
	if stats.CurrentBytes != 15 {
		t.Errorf("expected 15 bytes via SizeFunc, got %d", stats.CurrentBytes)
	}

	// Explicit size should override.
	c.Put("c", "x", 50)
	stats = c.Stats()
	if stats.CurrentBytes != 65 {
		t.Errorf("expected 65 bytes, got %d", stats.CurrentBytes)
	}
}

// --- GetOrLoad / singleflight tests ---

func TestARC_GetOrLoad_Hit(t *testing.T) {
	c := NewARCCache[string, int](10, 0)
	c.Put("a", 42, 8)

	val, err := c.GetOrLoad("a", func(_ string) (int, int64, error) {
		t.Fatal("loader should not be called on cache hit")
		return 0, 0, nil
	})
	if err != nil || val != 42 {
		t.Errorf("expected (42, nil), got (%d, %v)", val, err)
	}
}

func TestARC_GetOrLoad_Miss(t *testing.T) {
	c := NewARCCache[string, int](10, 0)

	val, err := c.GetOrLoad("a", func(k string) (int, int64, error) {
		if k != "a" {
			t.Errorf("loader received wrong key: %q", k)
		}
		return 99, 8, nil
	})
	if err != nil || val != 99 {
		t.Errorf("expected (99, nil), got (%d, %v)", val, err)
	}

	// Should now be cached.
	val, ok := c.Get("a")
	if !ok || val != 99 {
		t.Errorf("expected cached value 99, got %d (ok=%v)", val, ok)
	}
}

func TestARC_GetOrLoad_Error(t *testing.T) {
	c := NewARCCache[string, int](10, 0)

	_, err := c.GetOrLoad("a", func(_ string) (int, int64, error) {
		return 0, 0, errors.New("db down")
	})
	if err == nil || err.Error() != "db down" {
		t.Errorf("expected error 'db down', got %v", err)
	}

	// Should NOT be cached.
	if _, ok := c.Get("a"); ok {
		t.Error("expected error result to not be cached")
	}
}

func TestARC_GetOrLoad_Singleflight(t *testing.T) {
	c := NewARCCache[string, int](10, 0)

	var calls atomic.Int32
	var wg sync.WaitGroup

	loader := func(_ string) (int, int64, error) {
		calls.Add(1)
		return 77, 8, nil
	}

	// Launch 50 concurrent loads for the same key.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, err := c.GetOrLoad("x", loader)
			if err != nil || val != 77 {
				t.Errorf("unexpected result: %d, %v", val, err)
			}
		}()
	}

	wg.Wait()

	// Singleflight should deduplicate — at most a few calls.
	if n := calls.Load(); n > 5 {
		t.Errorf("expected singleflight dedup; loader called %d times (should be ~1)", n)
	}
}

// --- entry pool tests ---

func TestARC_EntryPool(t *testing.T) {
	// Fill and evict entries; the pool should recycle objects.
	c := NewARCCache[int, int](2, 0)

	for i := 0; i < 100; i++ {
		c.Put(i, i, 8)
	}

	// No panic = pool works. Verify final state.
	stats := c.Stats()
	if stats.Entries != 2 {
		t.Errorf("expected 2 entries, got %d", stats.Entries)
	}
}

// --- extractWriteTable tests ---

func TestExtractWriteTable(t *testing.T) {
	tests := []struct {
		query string
		table string
	}{
		{"INSERT INTO users (name) VALUES ('a')", "USERS"},
		{"insert into orders values (1)", "ORDERS"},
		{"UPDATE products SET price = 10 WHERE id = 1", "PRODUCTS"},
		{"DELETE FROM sessions WHERE expired = true", "SESSIONS"},
		{"DROP TABLE temp_data", "TEMP_DATA"},
		{"ALTER TABLE users ADD COLUMN age INT", "USERS"},
		{"TRUNCATE TABLE logs", "LOGS"},
		{"CREATE TABLE IF NOT EXISTS cache (k TEXT, v TEXT)", "CACHE"},
		{"REPLACE INTO users (id, name) VALUES (1, 'a')", "USERS"},
		{"WITH t AS (SELECT 1) INSERT INTO results SELECT * FROM t", "RESULTS"},
		// Schema-qualified
		{"INSERT INTO public.users (name) VALUES ('a')", "USERS"},
		// Cannot determine table
		{"CALL my_proc()", ""},
	}

	for _, tt := range tests {
		got := extractWriteTable(tt.query)
		if got != tt.table {
			t.Errorf("extractWriteTable(%q) = %q, want %q", tt.query, got, tt.table)
		}
	}
}

// --- extractTableNames tests ---

func TestExtractTableNames(t *testing.T) {
	tests := []struct {
		query  string
		tables []string
	}{
		{"SELECT * FROM users", []string{"USERS"}},
		{"SELECT * FROM users JOIN orders ON users.id = orders.user_id", []string{"USERS", "ORDERS"}},
		{"SELECT * FROM users u LEFT JOIN orders o ON u.id = o.uid", []string{"USERS", "ORDERS"}},
		{"SELECT 1", nil},
		{"SELECT * FROM (SELECT 1) t", nil}, // subselect, not a table
	}

	for _, tt := range tests {
		got := extractTableNames(tt.query)
		if len(got) != len(tt.tables) {
			t.Errorf("extractTableNames(%q) = %v, want %v", tt.query, got, tt.tables)
			continue
		}
		for i := range got {
			if got[i] != tt.tables[i] {
				t.Errorf("extractTableNames(%q)[%d] = %q, want %q", tt.query, i, got[i], tt.tables[i])
			}
		}
	}
}

// --- functional options tests ---

func TestARC_WithMaxBytes(t *testing.T) {
	c := NewARCCache[string, int](100, 0, WithMaxBytes[string, int](48))

	c.Put("a", 1, 16)
	c.Put("b", 2, 16)
	c.Put("c", 3, 16)
	c.Put("d", 4, 16) // should trigger memory eviction

	stats := c.Stats()
	if stats.CurrentBytes > 48 {
		t.Errorf("expected <= 48 bytes, got %d", stats.CurrentBytes)
	}
}
