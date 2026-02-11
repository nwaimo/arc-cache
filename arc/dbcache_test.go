package arc

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

// newTestDB creates an in-memory SQLite database with a test table.
func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO users (id, name, email) VALUES
			(1, 'Alice', 'alice@example.com'),
			(2, 'Bob', 'bob@example.com'),
			(3, 'Charlie', 'charlie@example.com')
	`)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	return db
}

func TestCachedDB_SelectIsCached(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	cached := NewCachedDB(db, 10, 0)
	defer cached.Close()

	ctx := context.Background()

	result1, err := cached.Query(ctx, "SELECT * FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result1.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result1.Rows))
	}

	stats := cached.Stats()
	if stats.Misses != 1 || stats.Hits != 0 {
		t.Errorf("expected 1 miss, 0 hits; got %d misses, %d hits",
			stats.Misses, stats.Hits)
	}

	result2, err := cached.Query(ctx, "SELECT * FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	stats = cached.Stats()
	if stats.Hits != 1 {
		t.Errorf("expected 1 hit after second query, got %d", stats.Hits)
	}

	if result1 != result2 {
		t.Error("expected cached result to be same pointer")
	}
}

func TestCachedDB_DifferentArgsAreSeparate(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	cached := NewCachedDB(db, 10, 0)
	defer cached.Close()

	ctx := context.Background()

	r1, err := cached.Query(ctx, "SELECT * FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatal(err)
	}

	r2, err := cached.Query(ctx, "SELECT * FROM users WHERE id = ?", 2)
	if err != nil {
		t.Fatal(err)
	}

	if r1 == r2 {
		t.Error("expected different results for different args")
	}

	stats := cached.Stats()
	if stats.Misses != 2 {
		t.Errorf("expected 2 misses, got %d", stats.Misses)
	}
}

func TestCachedDB_InsertInvalidatesCache(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	cached := NewCachedDB(db, 10, 0)
	defer cached.Close()

	ctx := context.Background()

	// Warm the cache.
	_, err := cached.Query(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}

	stats := cached.Stats()
	if stats.Entries != 1 {
		t.Fatalf("expected 1 cached entry, got %d", stats.Entries)
	}

	// INSERT invalidates entries that reference the "users" table.
	_, err = cached.Exec(ctx, "INSERT INTO users (id, name, email) VALUES (4, 'Dave', 'dave@example.com')")
	if err != nil {
		t.Fatal(err)
	}

	stats = cached.Stats()
	if stats.Entries != 0 {
		t.Errorf("expected 0 entries after INSERT, got %d", stats.Entries)
	}

	// Re-query should now include the new row.
	result, err := cached.Query(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 4 {
		t.Errorf("expected 4 rows after INSERT, got %d", len(result.Rows))
	}
}

func TestCachedDB_UpdateInvalidatesCache(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	cached := NewCachedDB(db, 10, 0)
	defer cached.Close()

	ctx := context.Background()

	_, err := cached.Query(ctx, "SELECT name FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cached.Exec(ctx, "UPDATE users SET name = 'Alicia' WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}

	stats := cached.Stats()
	if stats.Entries != 0 {
		t.Errorf("expected 0 entries after UPDATE, got %d", stats.Entries)
	}

	result, err := cached.Query(ctx, "SELECT name FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatal("expected 1 row")
	}
	name, ok := result.Rows[0][0].(string)
	if !ok || name != "Alicia" {
		t.Errorf("expected 'Alicia', got %v", result.Rows[0][0])
	}
}

func TestCachedDB_DeleteQueryInvalidatesCache(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	cached := NewCachedDB(db, 10, 0)
	defer cached.Close()

	ctx := context.Background()

	result, err := cached.Query(ctx, "DELETE FROM users WHERE id = ?", 3)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows for DELETE result, got %d", len(result.Rows))
	}

	stats := cached.Stats()
	if stats.Entries != 0 {
		t.Errorf("expected 0 entries after DELETE via Query, got %d", stats.Entries)
	}
}

func TestCachedDB_ManualInvalidate(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	cached := NewCachedDB(db, 10, 0)
	defer cached.Close()

	ctx := context.Background()

	_, err := cached.Query(ctx, "SELECT * FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatal(err)
	}

	stats := cached.Stats()
	if stats.Entries != 1 {
		t.Fatalf("expected 1 entry, got %d", stats.Entries)
	}

	cached.Invalidate("SELECT * FROM users WHERE id = ?", 1)

	stats = cached.Stats()
	if stats.Entries != 0 {
		t.Errorf("expected 0 entries after invalidation, got %d", stats.Entries)
	}
}

func TestCachedDB_InvalidateAll(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	cached := NewCachedDB(db, 10, 0)
	defer cached.Close()

	ctx := context.Background()

	_, _ = cached.Query(ctx, "SELECT * FROM users WHERE id = ?", 1)
	_, _ = cached.Query(ctx, "SELECT * FROM users WHERE id = ?", 2)
	_, _ = cached.Query(ctx, "SELECT * FROM users")

	stats := cached.Stats()
	if stats.Entries != 3 {
		t.Fatalf("expected 3 entries, got %d", stats.Entries)
	}

	cached.InvalidateAll()

	stats = cached.Stats()
	if stats.Entries != 0 {
		t.Errorf("expected 0 entries after InvalidateAll, got %d", stats.Entries)
	}
}

func TestCachedDB_DisableCache(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	cached := NewCachedDB(db, 10, 0)
	defer cached.Close()

	ctx := context.Background()

	cached.SetEnabled(false)

	_, err := cached.Query(ctx, "SELECT * FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatal(err)
	}

	stats := cached.Stats()
	if stats.Entries != 0 {
		t.Errorf("expected 0 entries when cache disabled, got %d", stats.Entries)
	}

	cached.SetEnabled(true)

	_, err = cached.Query(ctx, "SELECT * FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatal(err)
	}

	stats = cached.Stats()
	if stats.Entries != 1 {
		t.Errorf("expected 1 entry after re-enabling, got %d", stats.Entries)
	}
}

func TestCachedDB_QueryResult_Columns(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	cached := NewCachedDB(db, 10, 0)
	defer cached.Close()

	ctx := context.Background()

	result, err := cached.Query(ctx, "SELECT id, name, email FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}

	expectedCols := []string{"id", "name", "email"}
	if len(result.Columns) != len(expectedCols) {
		t.Fatalf("expected %d columns, got %d", len(expectedCols), len(result.Columns))
	}
	for i, col := range result.Columns {
		if col != expectedCols[i] {
			t.Errorf("column %d: expected %q, got %q", i, expectedCols[i], col)
		}
	}

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
}

// --- Table-level invalidation tests ---

func TestCachedDB_TableLevelInvalidation(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	// Create a second table.
	_, err := db.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO orders VALUES (1, 1, 9.99), (2, 2, 19.99)`)
	if err != nil {
		t.Fatal(err)
	}

	cached := NewCachedDB(db, 10, 0)
	defer cached.Close()

	ctx := context.Background()

	// Warm cache with queries for both tables.
	_, _ = cached.Query(ctx, "SELECT * FROM users")
	_, _ = cached.Query(ctx, "SELECT * FROM orders")

	stats := cached.Stats()
	if stats.Entries != 2 {
		t.Fatalf("expected 2 entries, got %d", stats.Entries)
	}

	// INSERT into orders should only invalidate the orders entry.
	_, err = cached.Exec(ctx, "INSERT INTO orders (id, user_id, amount) VALUES (3, 3, 29.99)")
	if err != nil {
		t.Fatal(err)
	}

	stats = cached.Stats()
	if stats.Entries != 1 {
		t.Errorf("expected 1 entry (users still cached), got %d", stats.Entries)
	}

	// Verify the users query is still cached (hit).
	_, err = cached.Query(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	stats = cached.Stats()
	if stats.Hits < 1 {
		t.Error("expected users query to still be cached after orders INSERT")
	}
}

func TestCachedDB_InvalidateTable(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	cached := NewCachedDB(db, 10, 0)
	defer cached.Close()

	ctx := context.Background()

	_, _ = cached.Query(ctx, "SELECT * FROM users WHERE id = ?", 1)
	_, _ = cached.Query(ctx, "SELECT * FROM users WHERE id = ?", 2)

	stats := cached.Stats()
	if stats.Entries != 2 {
		t.Fatalf("expected 2 entries, got %d", stats.Entries)
	}

	cached.InvalidateTable("users")

	stats = cached.Stats()
	if stats.Entries != 0 {
		t.Errorf("expected 0 entries after InvalidateTable, got %d", stats.Entries)
	}
}
