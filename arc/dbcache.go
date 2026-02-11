package arc

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"strings"
	"sync"
	"time"
)

// QueryResult holds the materialized result of a SQL SELECT query.
type QueryResult struct {
	Columns []string // column names
	Rows    [][]any  // row data; each inner slice corresponds to one row
}

// CachedDB wraps a *sql.DB with an ARC cache for read queries.
// Write queries (INSERT, UPDATE, DELETE, etc.) automatically invalidate
// only the cache entries associated with the affected table.
type CachedDB struct {
	db    *sql.DB
	cache *ARCCache[uint64, *QueryResult]

	mu      sync.RWMutex // protects enabled flag and tableKeys
	enabled bool         // whether caching is active

	// tableKeys maps a normalized table name to the set of cache keys
	// that reference that table. Used for table-level invalidation.
	tableKeys map[string]map[uint64]struct{}

	// keyTables maps a cache key to the set of table names referenced by
	// the query that produced it. Used to clean up tableKeys on eviction.
	keyTables map[uint64]map[string]struct{}
}

// writeKeywords lists SQL keywords that indicate a write operation.
var writeKeywords = map[string]bool{
	"INSERT":   true,
	"UPDATE":   true,
	"DELETE":   true,
	"DROP":     true,
	"ALTER":    true,
	"TRUNCATE": true,
	"CREATE":   true,
	"REPLACE":  true,
	"MERGE":    true,
	"UPSERT":   true,
	"GRANT":    true,
	"REVOKE":   true,
	"CALL":     true,
}

// NewCachedDB creates a new CachedDB wrapping the given *sql.DB.
//
// Parameters:
//   - db: the underlying database connection
//   - maxEntries: maximum number of cached query results
//   - maxBytes: maximum total memory for cached results (0 = no memory limit)
func NewCachedDB(db *sql.DB, maxEntries int, maxBytes int64) *CachedDB {
	cdb := &CachedDB{
		db:        db,
		enabled:   true,
		tableKeys: make(map[string]map[uint64]struct{}),
		keyTables: make(map[uint64]map[string]struct{}),
	}

	// Build the ARC cache with auto-sizing and an eviction callback to
	// clean up the table-key index.
	cdb.cache = NewARCCache[uint64, *QueryResult](maxEntries, maxBytes,
		WithSizeFunc[uint64, *QueryResult](func(_ uint64, v *QueryResult) int64 {
			return EstimateSize(v)
		}),
		WithOnEvict[uint64, *QueryResult](func(key uint64, _ *QueryResult) {
			cdb.removeKeyFromIndex(key)
		}),
	)

	return cdb
}

// DB returns the underlying *sql.DB for direct access when needed.
func (c *CachedDB) DB() *sql.DB {
	return c.db
}

// SetEnabled enables or disables the cache. When disabled, all queries
// go directly to the database.
func (c *CachedDB) SetEnabled(enabled bool) {
	c.mu.Lock()
	c.enabled = enabled
	c.mu.Unlock()
}

// Query executes a SQL query and returns the result.
//
// For read queries (SELECT, etc.), the result is cached. Subsequent identical
// queries return the cached result without hitting the database.
//
// For write queries (INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE),
// the query is executed directly and affected tables are invalidated.
func (c *CachedDB) Query(ctx context.Context, query string, args ...any) (*QueryResult, error) {
	// Fast prefix check: if first non-whitespace byte could be a write keyword,
	// do full tokenization. Otherwise skip SQL parsing entirely.
	if isWriteQueryFast(query) {
		result, err := c.executeQuery(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		c.invalidateForWrite(query)
		return result, nil
	}

	// Read path — check cache first.
	c.mu.RLock()
	cacheOn := c.enabled
	c.mu.RUnlock()

	key := cacheKey(query, args...)

	if cacheOn {
		if cached, ok := c.cache.Get(key); ok {
			return cached, nil
		}
	}

	// Cache miss — execute query.
	result, err := c.executeQuery(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	if cacheOn {
		c.cache.Put(key, result, 0) // size auto-computed via SizeFunc
		c.indexQueryTables(key, query)
	}

	return result, nil
}

// Exec executes a SQL statement that doesn't return rows (INSERT, UPDATE, DELETE, etc.).
// After successful execution, the affected table's cache entries are invalidated.
func (c *CachedDB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	c.invalidateForWrite(query)
	return result, nil
}

// Invalidate removes a specific cached query result by its query string and args.
func (c *CachedDB) Invalidate(query string, args ...any) {
	key := cacheKey(query, args...)
	c.cache.Delete(key)
	c.removeKeyFromIndex(key)
}

// InvalidateByKey removes a specific cached query result by its uint64 cache key.
func (c *CachedDB) InvalidateByKey(key uint64) {
	c.cache.Delete(key)
	c.removeKeyFromIndex(key)
}

// InvalidateTable invalidates all cached queries that reference the given table.
func (c *CachedDB) InvalidateTable(table string) {
	norm := strings.ToUpper(strings.TrimSpace(table))
	c.mu.Lock()
	keys := c.tableKeys[norm]
	toDelete := make([]uint64, 0, len(keys))
	for k := range keys {
		toDelete = append(toDelete, k)
	}
	c.mu.Unlock()

	for _, k := range toDelete {
		c.cache.Delete(k)
		c.removeKeyFromIndex(k)
	}
}

// InvalidateAll clears the entire cache.
func (c *CachedDB) InvalidateAll() {
	c.cache.Clear()
	c.mu.Lock()
	c.tableKeys = make(map[string]map[uint64]struct{})
	c.keyTables = make(map[uint64]map[string]struct{})
	c.mu.Unlock()
}

// Stats returns the current cache statistics.
func (c *CachedDB) Stats() CacheStats {
	return c.cache.Stats()
}

// Close clears the cache and closes the underlying database connection.
func (c *CachedDB) Close() error {
	c.InvalidateAll()
	return c.db.Close()
}

// --- table-level invalidation helpers ---

// indexQueryTables extracts table names from a read query and records
// the mapping between cache key and table names.
func (c *CachedDB) indexQueryTables(key uint64, query string) {
	tokens := sqlTokens(query)
	tables := extractTableNamesFromTokens(tokens)
	if len(tables) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	tSet := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		tSet[t] = struct{}{}
		if c.tableKeys[t] == nil {
			c.tableKeys[t] = make(map[uint64]struct{})
		}
		c.tableKeys[t][key] = struct{}{}
	}
	c.keyTables[key] = tSet
}

// removeKeyFromIndex removes a cache key from the table-key index.
func (c *CachedDB) removeKeyFromIndex(key uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tables := c.keyTables[key]
	for t := range tables {
		delete(c.tableKeys[t], key)
		if len(c.tableKeys[t]) == 0 {
			delete(c.tableKeys, t)
		}
	}
	delete(c.keyTables, key)
}

// invalidateForWrite extracts the target table from a write query
// and invalidates only cache entries that reference that table.
// Falls back to full cache clear if the table cannot be determined.
// Tokenizes once and reuses the tokens.
func (c *CachedDB) invalidateForWrite(query string) {
	tokens := sqlTokens(query)
	table := extractWriteTableFromTokens(tokens)
	if table == "" {
		c.InvalidateAll()
		return
	}
	c.InvalidateTable(table)
}

// executeQuery runs a SELECT-style query and materializes all rows into a QueryResult.
func (c *CachedDB) executeQuery(ctx context.Context, query string, args ...any) (*QueryResult, error) {
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("arc/dbcache: query failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("arc/dbcache: columns failed: %w", err)
	}

	result := &QueryResult{
		Columns: columns,
	}

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("arc/dbcache: scan failed: %w", err)
		}

		row := make([]any, len(columns))
		for i, v := range values {
			row[i] = copyValue(v)
		}
		result.Rows = append(result.Rows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("arc/dbcache: rows iteration failed: %w", err)
	}

	return result, nil
}

// --- cache key generation ---

// hasherPool reuses FNV-1a hashers across calls to avoid heap allocation per query.
var hasherPool = sync.Pool{
	New: func() any { return fnv.New64a() },
}

// cacheKey generates a deterministic FNV-1a hash from the query string and arguments.
// Returns a uint64 directly — no string conversion, no allocations beyond the pool.
func cacheKey(query string, args ...any) uint64 {
	h := hasherPool.Get().(hash.Hash64)
	h.Reset()
	h.Write([]byte(query))
	var buf [32]byte // stack-allocated scratch buffer for numeric formatting
	for _, arg := range args {
		h.Write([]byte{'\x00'})
		writeTypedArg(h, arg, buf[:])
	}
	val := h.Sum64()
	hasherPool.Put(h)
	return val
}

// writeTypedArg writes a type-prefixed representation of arg to the hasher.
// Uses the caller-provided scratch buffer to avoid heap allocations.
func writeTypedArg(h hash.Hash64, arg any, buf []byte) {
	if arg == nil {
		h.Write([]byte("nil"))
		return
	}
	switch v := arg.(type) {
	case string:
		h.Write([]byte("s:"))
		h.Write([]byte(v))
	case int:
		h.Write([]byte("i:"))
		binary.LittleEndian.PutUint64(buf, uint64(v))
		h.Write(buf[:8])
	case int64:
		h.Write([]byte("I:"))
		binary.LittleEndian.PutUint64(buf, uint64(v))
		h.Write(buf[:8])
	case int32:
		h.Write([]byte("i32:"))
		binary.LittleEndian.PutUint32(buf, uint32(v))
		h.Write(buf[:4])
	case float64:
		h.Write([]byte("f:"))
		binary.LittleEndian.PutUint64(buf, uint64(v))
		h.Write(buf[:8])
	case float32:
		h.Write([]byte("f32:"))
		binary.LittleEndian.PutUint32(buf, uint32(v))
		h.Write(buf[:4])
	case bool:
		if v {
			h.Write([]byte("b:1"))
		} else {
			h.Write([]byte("b:0"))
		}
	case []byte:
		h.Write([]byte("B:"))
		h.Write(v)
	case time.Time:
		h.Write([]byte("t:"))
		binary.LittleEndian.PutUint64(buf, uint64(v.UnixNano()))
		h.Write(buf[:8])
	default:
		fmt.Fprintf(h, "%T:%v", arg, arg)
	}
}

// --- SQL parsing helpers ---

// isWriteQueryFast uses a cheap prefix check to skip full SQL parsing for
// the most common case (SELECT / SHOW / EXPLAIN). Only falls back to
// full tokenization for ambiguous prefixes like WITH (CTE + possible DML).
func isWriteQueryFast(query string) bool {
	// Skip leading whitespace.
	s := query
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\n' || s[0] == '\r') {
		s = s[1:]
	}
	if len(s) == 0 {
		return false
	}
	// Lowercase the first character for comparison.
	ch := s[0] | 0x20
	switch ch {
	case 's': // SELECT, SHOW, SET — always reads
		return false
	case 'e': // EXPLAIN — always a read
		return false
	case 'd': // DESCRIBE — read; DELETE, DROP — write
		// Check for DESCRIBE vs DELETE/DROP
		if len(s) >= 2 && (s[1]|0x20) == 'e' {
			if len(s) >= 3 && (s[2]|0x20) == 's' { // DES... → DESCRIBE
				return false
			}
			return true // DELETE
		}
		return true // DROP or other D-word
	case 'w': // WITH — ambiguous (CTE could wrap a write)
		return isWriteQuery(query)
	default:
		// i=INSERT, u=UPDATE/UPSERT, c=CREATE/CALL, a=ALTER,
		// t=TRUNCATE, r=REPLACE/REVOKE, m=MERGE, g=GRANT
		// All are writes. Unknown prefixes → check fully.
		return isWriteQuery(query)
	}
}

// isWriteQuery does full SQL tokenization to detect write operations.
// Called only when the fast prefix check is ambiguous.
func isWriteQuery(query string) bool {
	tokens := sqlTokens(query)
	return isWriteFromTokens(tokens)
}

// isWriteFromTokens checks pre-tokenized SQL for write keywords.
func isWriteFromTokens(tokens []string) bool {
	if len(tokens) == 0 {
		return false
	}

	first := tokens[0]

	if writeKeywords[first] {
		return true
	}

	if first == "WITH" {
		for _, tok := range tokens[1:] {
			if writeKeywords[tok] {
				return true
			}
		}
	}

	return false
}

// extractWriteTable returns the normalized table name from a write query.
// Returns "" if the table cannot be determined.
func extractWriteTable(query string) string {
	tokens := sqlTokens(query)
	return extractWriteTableFromTokens(tokens)
}

// extractWriteTableFromTokens extracts the target table from pre-tokenized SQL.
func extractWriteTableFromTokens(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}

	// For CTE writes, skip to the actual DML keyword.
	start := 0
	if tokens[0] == "WITH" {
		for i, tok := range tokens {
			if writeKeywords[tok] && tok != "CALL" && tok != "GRANT" && tok != "REVOKE" {
				start = i
				break
			}
		}
	}

	if start >= len(tokens) {
		return ""
	}

	kw := tokens[start]
	switch kw {
	case "INSERT", "REPLACE", "MERGE":
		// INSERT INTO table / REPLACE INTO table / MERGE INTO table
		for i := start + 1; i < len(tokens); i++ {
			if tokens[i] == "INTO" && i+1 < len(tokens) {
				return stripSchema(tokens[i+1])
			}
		}
		// INSERT table (without INTO)
		if start+1 < len(tokens) {
			return stripSchema(tokens[start+1])
		}
	case "UPDATE":
		// UPDATE table SET ...
		if start+1 < len(tokens) {
			return stripSchema(tokens[start+1])
		}
	case "DELETE":
		// DELETE FROM table
		for i := start + 1; i < len(tokens); i++ {
			if tokens[i] == "FROM" && i+1 < len(tokens) {
				return stripSchema(tokens[i+1])
			}
		}
	case "DROP", "ALTER", "TRUNCATE":
		// DROP TABLE table / ALTER TABLE table / TRUNCATE TABLE table
		for i := start + 1; i < len(tokens); i++ {
			if tokens[i] == "TABLE" && i+1 < len(tokens) {
				tok := tokens[i+1]
				// Skip IF EXISTS / IF NOT EXISTS
				if tok == "IF" {
					for j := i + 2; j < len(tokens); j++ {
						if tokens[j] != "EXISTS" && tokens[j] != "NOT" && tokens[j] != "IF" {
							return stripSchema(tokens[j])
						}
					}
				}
				return stripSchema(tok)
			}
		}
	case "CREATE":
		// CREATE TABLE table — also handle CREATE TABLE IF NOT EXISTS
		for i := start + 1; i < len(tokens); i++ {
			if tokens[i] == "TABLE" && i+1 < len(tokens) {
				tok := tokens[i+1]
				if tok == "IF" {
					for j := i + 2; j < len(tokens); j++ {
						if tokens[j] != "EXISTS" && tokens[j] != "NOT" && tokens[j] != "IF" {
							return stripSchema(tokens[j])
						}
					}
				}
				return stripSchema(tok)
			}
		}
	case "UPSERT":
		if start+1 < len(tokens) {
			next := tokens[start+1]
			if next == "INTO" && start+2 < len(tokens) {
				return stripSchema(tokens[start+2])
			}
			return stripSchema(next)
		}
	}

	return ""
}

// extractTableNames extracts table names referenced in a read query.
func extractTableNames(query string) []string {
	tokens := sqlTokens(query)
	return extractTableNamesFromTokens(tokens)
}

// extractTableNamesFromTokens extracts table names from pre-tokenized SQL.
// It looks for FROM and JOIN keywords followed by a table name.
func extractTableNamesFromTokens(tokens []string) []string {
	seen := make(map[string]bool)
	var tables []string

	for i, tok := range tokens {
		if (tok == "FROM" || tok == "JOIN") && i+1 < len(tokens) {
			next := tokens[i+1]
			if next == "(" || next == "SELECT" {
				continue
			}
			name := stripSchema(next)
			if name != "" && !seen[name] {
				seen[name] = true
				tables = append(tables, name)
			}
		}
	}
	return tables
}

// sqlSepReplacer replaces common SQL separators with spaces.
// Hoisted to package level to avoid allocating a new Replacer per call.
var sqlSepReplacer = strings.NewReplacer("(", " ", ")", " ", ",", " ", ";", " ", "\t", " ", "\r", " ")

// sqlTokens splits a SQL string into uppercase tokens, stripping
// comments, parentheses, commas, and semicolons.
func sqlTokens(query string) []string {
	s := strings.TrimSpace(query)
	s = strings.ToUpper(s)
	// Remove single-line comments
	if strings.Contains(s, "--") {
		var b strings.Builder
		b.Grow(len(s))
		for len(s) > 0 {
			nl := strings.IndexByte(s, '\n')
			var line string
			if nl < 0 {
				line = s
				s = ""
			} else {
				line = s[:nl]
				s = s[nl+1:]
			}
			if idx := strings.Index(line, "--"); idx >= 0 {
				line = line[:idx]
			}
			b.WriteString(line)
			b.WriteByte(' ')
		}
		s = b.String()
	}
	// Remove block comments (simple non-nested)
	for {
		start := strings.Index(s, "/*")
		if start < 0 {
			break
		}
		end := strings.Index(s[start:], "*/")
		if end < 0 {
			s = s[:start]
			break
		}
		s = s[:start] + " " + s[start+end+2:]
	}
	s = sqlSepReplacer.Replace(s)
	return strings.Fields(s)
}

// stripSchema removes a "schema." prefix and any backtick/quote wrapping
// from a table name.
func stripSchema(name string) string {
	// Remove quoting chars: ", `, [, ]
	name = strings.Trim(name, "\"`[]")
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	return name
}

// copyValue creates a copy of scanned values to avoid aliasing with
// driver-internal buffers.
func copyValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		cp := make([]byte, len(val))
		copy(cp, val)
		return cp
	default:
		return v
	}
}
