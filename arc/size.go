package arc

import (
	"reflect"
	"unsafe"
)

const (
	// Overhead per row in a QueryResult (slice header + padding).
	perRowOverhead = 64
	// Overhead per cell (interface header).
	perCellOverhead = 16
	// Base overhead for a QueryResult struct itself.
	queryResultBaseOverhead = 128
)

// EstimateSize calculates an approximate memory footprint in bytes for a QueryResult.
// This is used by the ARC cache to enforce the maxBytes memory limit.
// The estimate includes:
//   - base struct overhead
//   - column name strings
//   - per-row slice header overhead
//   - per-cell value sizes (strings by length, numerics by type width, etc.)
func EstimateSize(result *QueryResult) int64 {
	if result == nil {
		return 0
	}

	var total int64 = queryResultBaseOverhead

	// Column names.
	for _, col := range result.Columns {
		total += int64(len(col)) + 16 // string header + content
	}

	// Rows.
	for _, row := range result.Rows {
		total += perRowOverhead
		for _, cell := range row {
			total += perCellOverhead + estimateCellSize(cell)
		}
	}

	return total
}

// estimateCellSize returns the approximate memory size of a single cell value.
func estimateCellSize(v any) int64 {
	if v == nil {
		return 0
	}

	switch val := v.(type) {
	case string:
		return int64(len(val))
	case []byte:
		return int64(len(val))
	case bool:
		return 1
	case int8, uint8:
		return 1
	case int16, uint16:
		return 2
	case int32, uint32, float32:
		return 4
	case int, uint, int64, uint64, float64:
		return 8
	case complex64:
		return 8
	case complex128:
		return 16
	default:
		// Fallback: use reflect to get the type size.
		rv := reflect.ValueOf(v)
		if rv.IsValid() {
			return int64(unsafe.Sizeof(v)) + int64(rv.Type().Size())
		}
		return 32 // conservative default
	}
}
