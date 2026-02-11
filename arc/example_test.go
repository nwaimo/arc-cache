package arc_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"arc-cache/arc"

	_ "modernc.org/sqlite"
)

func Example_basicUsage() {
	// Open a database connection.
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create test schema and data.
	db.Exec(`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`)
	db.Exec(`INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99), (3, 'Doohickey', 4.99)`)

	// Wrap the database with an ARC cache.
	// Max 1000 cached query results, max 64MB memory.
	cached := arc.NewCachedDB(db, 1000, 64*1024*1024)
	defer cached.Close()

	ctx := context.Background()

	// First query — cache miss, hits the database.
	result, err := cached.Query(ctx, "SELECT name, price FROM products WHERE price > ?", 5.0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Query 1: %d rows (from DB)\n", len(result.Rows))

	// Second identical query — cache hit, no database access.
	result, err = cached.Query(ctx, "SELECT name, price FROM products WHERE price > ?", 5.0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Query 2: %d rows (from cache)\n", len(result.Rows))

	// Check stats.
	stats := cached.Stats()
	fmt.Printf("Hits: %d, Misses: %d, Entries: %d\n", stats.Hits, stats.Misses, stats.Entries)

	// Performing a write automatically invalidates the cache.
	_, err = cached.Exec(ctx, "INSERT INTO products VALUES (4, 'Thingamajig', 14.99)")
	if err != nil {
		log.Fatal(err)
	}

	stats = cached.Stats()
	fmt.Printf("After INSERT — Entries: %d (cache cleared)\n", stats.Entries)

	// Next query hits the database again and includes the new row.
	result, err = cached.Query(ctx, "SELECT name, price FROM products WHERE price > ?", 5.0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Query 3: %d rows (includes new product)\n", len(result.Rows))

	// Output:
	// Query 1: 2 rows (from DB)
	// Query 2: 2 rows (from cache)
	// Hits: 1, Misses: 1, Entries: 1
	// After INSERT — Entries: 0 (cache cleared)
	// Query 3: 3 rows (includes new product)
}

func Example_arcAdaptation() {
	// Demonstrate ARC's adaptive behavior with the raw cache.
	cache := arc.NewARCCache[string, string](4, 0)

	// Insert 4 items — all go to T1 (seen once).
	cache.Put("a", "A", 8)
	cache.Put("b", "B", 8)
	cache.Put("c", "C", 8)
	cache.Put("d", "D", 8)

	stats := cache.Stats()
	fmt.Printf("After 4 puts: T1=%d, T2=%d\n", stats.T1Len, stats.T2Len)

	// Access "a" and "b" — promotes them to T2 (seen twice).
	cache.Get("a")
	cache.Get("b")

	stats = cache.Stats()
	fmt.Printf("After accessing a,b: T1=%d, T2=%d\n", stats.T1Len, stats.T2Len)

	// Insert new items — ARC adapts p based on eviction patterns.
	cache.Put("e", "E", 8)
	cache.Put("f", "F", 8)

	stats = cache.Stats()
	fmt.Printf("After adding e,f: T1=%d, T2=%d, P=%d\n", stats.T1Len, stats.T2Len, stats.P)

	// Frequently accessed items ("a", "b") should survive longer.
	if _, ok := cache.Get("a"); ok {
		fmt.Println("'a' survived (frequent)")
	}
	if _, ok := cache.Get("b"); ok {
		fmt.Println("'b' survived (frequent)")
	}

	// Output:
	// After 4 puts: T1=4, T2=0
	// After accessing a,b: T1=2, T2=2
	// After adding e,f: T1=2, T2=2, P=0
	// 'a' survived (frequent)
	// 'b' survived (frequent)
}
