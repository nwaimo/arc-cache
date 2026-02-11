package arc

// entry represents a node in a doubly-linked list used by the ARC cache.
// K must be comparable for map lookups; V is the cached value type.
// listID identifies which ARC list an entry belongs to.
type listID int

const (
	listNone listID = iota
	listT1
	listT2
	listB1
	listB2
)

type entry[K comparable, V any] struct {
	key   K
	value V
	size  int64 // estimated memory footprint in bytes

	prev *entry[K, V]
	next *entry[K, V]

	// ghost indicates this entry only tracks the key (no value stored).
	// Ghost entries live in B1/B2 and consume no value memory.
	ghost bool

	// list tracks which ARC list this entry currently belongs to.
	list listID
}

// doublyLinkedList is a generic intrusive doubly-linked list with O(1)
// push-front, remove, move-to-front, and remove-back operations.
type doublyLinkedList[K comparable, V any] struct {
	head *entry[K, V]
	tail *entry[K, V]
	len  int
}

// Len returns the number of entries in the list.
func (l *doublyLinkedList[K, V]) Len() int {
	return l.len
}

// PushFront inserts e at the front of the list.
func (l *doublyLinkedList[K, V]) PushFront(e *entry[K, V]) {
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	}
	l.head = e
	if l.tail == nil {
		l.tail = e
	}
	l.len++
}

// Remove removes e from the list. The caller must ensure e belongs to this list.
func (l *doublyLinkedList[K, V]) Remove(e *entry[K, V]) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.prev = nil
	e.next = nil
	l.len--
}

// RemoveBack removes and returns the tail entry (LRU position).
// Returns nil if the list is empty.
func (l *doublyLinkedList[K, V]) RemoveBack() *entry[K, V] {
	if l.tail == nil {
		return nil
	}
	e := l.tail
	l.Remove(e)
	return e
}

// MoveToFront moves an existing entry to the front of the list.
func (l *doublyLinkedList[K, V]) MoveToFront(e *entry[K, V]) {
	if l.head == e {
		return // already at front
	}
	l.Remove(e)
	l.PushFront(e)
}

// Back returns the tail entry without removing it. Returns nil if empty.
func (l *doublyLinkedList[K, V]) Back() *entry[K, V] {
	return l.tail
}

// Front returns the head entry without removing it. Returns nil if empty.
func (l *doublyLinkedList[K, V]) Front() *entry[K, V] {
	return l.head
}

// Clear removes all entries from the list.
func (l *doublyLinkedList[K, V]) Clear() {
	l.head = nil
	l.tail = nil
	l.len = 0
}
