package arc

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	_ "modernc.org/sqlite"
)

// setupBenchDB creates an in-memory SQLite database with N rows for benchmarking.
func setupBenchDB(b *testing.B, numRows int) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatal(err)
	}

	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			category TEXT NOT NULL,
			price REAL NOT NULL,
			stock INTEGER NOT NULL
		)
	`)
	if err != nil {
		b.Fatal(err)
	}

	// Bulk insert using a transaction for speed.
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO products (id, name, category, price, stock) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}

	categories := []string{"electronics", "books", "clothing", "food", "toys"}
	for i := 1; i <= numRows; i++ {
		cat := categories[i%len(categories)]
		_, err = stmt.Exec(i, fmt.Sprintf("Product-%d", i), cat, float64(i)*1.99, i*10)
		if err != nil {
			b.Fatal(err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	// Create an index on category for realistic query performance.
	_, err = db.Exec("CREATE INDEX idx_category ON products(category)")
	if err != nil {
		b.Fatal(err)
	}

	return db
}

// --- Single-row lookup benchmarks ---

// BenchmarkUncached_SingleRow queries a single row by primary key, no cache.
func BenchmarkUncached_SingleRow(b *testing.B) {
	db := setupBenchDB(b, 1000)
	defer db.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := (i % 1000) + 1
		rows, err := db.QueryContext(ctx, "SELECT id, name, category, price, stock FROM products WHERE id = ?", id)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var pid, stock int
			var name, cat string
			var price float64
			rows.Scan(&pid, &name, &cat, &price, &stock)
		}
		rows.Close()
	}
}

// BenchmarkCached_SingleRow queries a single row by primary key, with ARC cache.
func BenchmarkCached_SingleRow(b *testing.B) {
	db := setupBenchDB(b, 1000)
	defer db.Close()

	cached := NewCachedDB(db, 500, 64*1024*1024)
	defer cached.Close()

	ctx := context.Background()

	// Warm the cache with all 1000 IDs (only 500 fit — ARC decides which survive).
	for i := 1; i <= 1000; i++ {
		cached.Query(ctx, "SELECT id, name, category, price, stock FROM products WHERE id = ?", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := (i % 1000) + 1
		_, err := cached.Query(ctx, "SELECT id, name, category, price, stock FROM products WHERE id = ?", id)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Multi-row range query benchmarks ---

// BenchmarkUncached_RangeQuery queries rows by category (returns ~200 rows each).
func BenchmarkUncached_RangeQuery(b *testing.B) {
	db := setupBenchDB(b, 1000)
	defer db.Close()

	ctx := context.Background()
	categories := []string{"electronics", "books", "clothing", "food", "toys"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat := categories[i%len(categories)]
		rows, err := db.QueryContext(ctx, "SELECT id, name, category, price, stock FROM products WHERE category = ?", cat)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var pid, stock int
			var name, c string
			var price float64
			rows.Scan(&pid, &name, &c, &price, &stock)
		}
		rows.Close()
	}
}

// BenchmarkCached_RangeQuery queries rows by category with ARC cache.
func BenchmarkCached_RangeQuery(b *testing.B) {
	db := setupBenchDB(b, 1000)
	defer db.Close()

	cached := NewCachedDB(db, 100, 64*1024*1024)
	defer cached.Close()

	ctx := context.Background()
	categories := []string{"electronics", "books", "clothing", "food", "toys"}

	// Warm cache.
	for _, cat := range categories {
		cached.Query(ctx, "SELECT id, name, category, price, stock FROM products WHERE category = ?", cat)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat := categories[i%len(categories)]
		_, err := cached.Query(ctx, "SELECT id, name, category, price, stock FROM products WHERE category = ?", cat)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Mixed workload benchmarks (simulates real app) ---

// BenchmarkUncached_MixedWorkload does 80% reads, 20% writes, no cache.
func BenchmarkUncached_MixedWorkload(b *testing.B) {
	db := setupBenchDB(b, 1000)
	defer db.Close()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rng.Float64() < 0.8 {
			// Read
			id := rng.Intn(1000) + 1
			rows, err := db.QueryContext(ctx, "SELECT id, name, category, price, stock FROM products WHERE id = ?", id)
			if err != nil {
				b.Fatal(err)
			}
			for rows.Next() {
				var pid, stock int
				var name, cat string
				var price float64
				rows.Scan(&pid, &name, &cat, &price, &stock)
			}
			rows.Close()
		} else {
			// Write
			id := rng.Intn(1000) + 1
			_, err := db.ExecContext(ctx, "UPDATE products SET stock = stock + 1 WHERE id = ?", id)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkCached_MixedWorkload does 80% reads, 20% writes, with ARC cache.
func BenchmarkCached_MixedWorkload(b *testing.B) {
	db := setupBenchDB(b, 1000)
	defer db.Close()

	cached := NewCachedDB(db, 500, 64*1024*1024)
	defer cached.Close()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rng.Float64() < 0.8 {
			// Read — goes through cache
			id := rng.Intn(1000) + 1
			_, err := cached.Query(ctx, "SELECT id, name, category, price, stock FROM products WHERE id = ?", id)
			if err != nil {
				b.Fatal(err)
			}
		} else {
			// Write — invalidates cache
			id := rng.Intn(1000) + 1
			_, err := cached.Exec(ctx, "UPDATE products SET stock = stock + 1 WHERE id = ?", id)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// --- Repeated same-query benchmark (best-case for caching) ---

// BenchmarkUncached_RepeatedQuery executes the exact same query repeatedly.
func BenchmarkUncached_RepeatedQuery(b *testing.B) {
	db := setupBenchDB(b, 1000)
	defer db.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, "SELECT id, name, category, price, stock FROM products WHERE id = ?", 42)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var pid, stock int
			var name, cat string
			var price float64
			rows.Scan(&pid, &name, &cat, &price, &stock)
		}
		rows.Close()
	}
}

// BenchmarkCached_RepeatedQuery executes the exact same query repeatedly (100% hit rate).
func BenchmarkCached_RepeatedQuery(b *testing.B) {
	db := setupBenchDB(b, 1000)
	defer db.Close()

	cached := NewCachedDB(db, 100, 64*1024*1024)
	defer cached.Close()

	ctx := context.Background()

	// Warm cache.
	cached.Query(ctx, "SELECT id, name, category, price, stock FROM products WHERE id = ?", 42)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cached.Query(ctx, "SELECT id, name, category, price, stock FROM products WHERE id = ?", 42)
		if err != nil {
			b.Fatal(err)
		}
	}
}
