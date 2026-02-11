# arc-cache

A high-performance **Adaptive Replacement Cache (ARC)** for Go, designed specifically for caching SQL database query results via `database/sql`.

## Features

- **ARC algorithm** — self-tuning cache that adapts between recency and frequency based on workload, outperforming LRU in virtually all access patterns
- **Generic core** — `ARCCache[K, V]` works with any comparable key and any value type
- **database/sql integration** — `CachedDB` wraps any `*sql.DB` driver (PostgreSQL, MySQL, SQLite, etc.)
- **Table-level invalidation** — write queries (INSERT/UPDATE/DELETE) automatically invalidate only cache entries that reference the affected table, preserving cache warmth for other tables
- **CTE & procedure support** — correctly detects `WITH ... INSERT/UPDATE/DELETE`, `CALL`, `GRANT/REVOKE` as write queries
- **Singleflight / GetOrLoad** — prevents thundering-herd on cache miss; concurrent requests for the same key share a single load
- **Dual capacity limits** — bounded by both entry count and memory (bytes), whichever is reached first
- **Functional options** — `WithMaxBytes`, `WithOnEvict`, `WithSizeFunc` for flexible configuration
- **Entry pooling** — reduces GC pressure by recycling evicted entry objects via `sync.Pool`-style pooling
- **Concurrency-safe** — `sync.Mutex` protects all cache state; passes `go test -race` (when CGO is available)
- **Zero external dependencies** for the core cache (only `database/sql` + stdlib)

## Installation

```bash
go get arc-cache/arc
```

## Quick Start

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"

    "arc-cache/arc"
    _ "github.com/lib/pq" // or any database/sql driver
)

func main() {
    db, err := sql.Open("postgres", "postgres://user:pass@localhost/mydb?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Wrap with ARC cache: 10,000 entries, 256MB memory limit
    cached := arc.NewCachedDB(db, 10_000, 256*1024*1024)
    defer cached.Close()

    ctx := context.Background()

    // First call hits the database
    result, _ := cached.Query(ctx, "SELECT name, email FROM users WHERE id = ?", 42)
    fmt.Println(result.Rows)

    // Second identical call returns from cache (sub-microsecond)
    result, _ = cached.Query(ctx, "SELECT name, email FROM users WHERE id = ?", 42)

    // Writes auto-invalidate only the affected table
    cached.Exec(ctx, "UPDATE users SET name = 'Alice' WHERE id = 42")

    // Stats
    stats := cached.Stats()
    fmt.Printf("Hits: %d, Misses: %d\n", stats.Hits, stats.Misses)
}
```

## Using the Raw ARC Cache

```go
cache := arc.NewARCCache[string, int](1000, 0,
    arc.WithMaxBytes[string, int](64*1024*1024),
    arc.WithOnEvict[string, int](func(k string, v int) {
        fmt.Printf("evicted: %s=%d\n", k, v)
    }),
    arc.WithSizeFunc[string, int](func(_ string, _ int) int64 {
        return 8
    }),
)

cache.Put("key", 42, 0) // size auto-computed by SizeFunc
val, ok := cache.Get("key")

// Singleflight loader
val, err := cache.GetOrLoad("key", func(k string) (int, int64, error) {
    // expensive computation or DB call
    return 42, 8, nil
})
```

## API Reference

### `ARCCache[K, V]`

| Method | Description |
|--------|-------------|
| `NewARCCache[K,V](maxEntries, maxBytes, ...Option)` | Create a new cache |
| `Get(key) (V, bool)` | Retrieve + promote (recency → frequency) |
| `Put(key, value, size)` | Insert or update |
| `Peek(key) (V, bool)` | Retrieve **without** promotion |
| `GetOrLoad(key, loader) (V, error)` | Get or compute with singleflight dedup |
| `Delete(key)` | Remove from cache |
| `Clear()` | Remove all entries |
| `Len() int` | Current entry count |
| `Keys() []K` | Snapshot of all cached keys |
| `Contains(key) bool` | Check existence without promotion |
| `Stats() CacheStats` | Hit/miss/size statistics |

### `CachedDB`

| Method | Description |
|--------|-------------|
| `NewCachedDB(db, maxEntries, maxBytes)` | Wrap a `*sql.DB` |
| `Query(ctx, query, args...) (*QueryResult, error)` | Cached read, auto-invalidating write |
| `Exec(ctx, query, args...) (sql.Result, error)` | Execute write + invalidate affected table |
| `Invalidate(query, args...)` | Remove a specific cached query |
| `InvalidateTable(table)` | Remove all cached queries for a table |
| `InvalidateAll()` | Clear entire cache |
| `SetEnabled(bool)` | Toggle caching on/off |
| `Stats() CacheStats` | Cache statistics |
| `DB() *sql.DB` | Access underlying connection |
| `Close() error` | Clear cache + close DB |

## Benchmarks

On Intel i5-8250U @ 1.60GHz, SQLite in-memory, 1000 rows:

| Benchmark | Uncached | Cached | Speedup |
|-----------|----------|--------|---------|
| Single row lookup | 82,744 ns/op | 63,712 ns/op | 1.3× |
| Range query (~200 rows) | 2,905,519 ns/op | 5,400 ns/op | **538×** |
| Repeated same query | 33,019 ns/op | 4,077 ns/op | **8.1×** |
| Mixed 80/20 read/write | 55,212 ns/op | 52,766 ns/op | 1.05× |

> Cached single-row lookups cycle through 1000 IDs with only 500 cache slots (50% hit rate). Repeated and range queries show the full benefit with 100% hit rates.

## Architecture

The ARC algorithm maintains four lists:

- **T1**: entries seen exactly once recently (recency)
- **T2**: entries seen at least twice recently (frequency)  
- **B1**: ghost entries evicted from T1 (metadata only)
- **B2**: ghost entries evicted from T2 (metadata only)

A parameter **p** dynamically shifts the balance between T1 and T2. When B1 gets ghost hits, p increases (favoring recency). When B2 gets ghost hits, p decreases (favoring frequency).

## License

MIT
