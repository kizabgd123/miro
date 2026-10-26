package telemetry

import (
	"errors"
	"runtime"
	"sync/atomic"
)

var (
	// ErrInvalidCapacity is returned when the capacity is not a power of 2.
	ErrInvalidCapacity = errors.New("telemetry: ring buffer capacity must be a power of 2 greater than zero")
	// ErrClosed is returned when an operation is attempted on a closed buffer.
	ErrClosed = errors.New("telemetry: ring buffer is closed")
)

// Slot holds a single TelemetryEvent along with a sequence version stamp.
// The version stamp acts as a lock-free seqlock:
// - Even (2*pos + 2): committed, safe to read if version matches ticket.
// - Odd  (2*pos + 1): in-flight write operation.
type Slot struct {
	version uint64
	event   TelemetryEvent
}

// RingBuffer is a high-performance, lock-free Multi-Producer Multi-Consumer (MPMC)
// ring buffer tailored for streaming telemetry events with overwrite-oldest semantics.
type RingBuffer struct {
	capacity uint64
	mask     uint64
	slots    []Slot

	// 64-byte cache line padding to prevent false sharing between hot atomic counters
	_pad0 [56]byte
	head  uint64 // monotonically increasing next write sequence

	_pad1 [56]byte
	tail  uint64 // monotonically increasing next read sequence

	_pad2 [56]byte
	dropped uint64 // total count of overwritten / dropped events

	_pad3 [56]byte
	closed uint32 // 1 if closed, 0 otherwise
}

// BufferStats captures a point-in-time snapshot of the ring buffer metrics.
type BufferStats struct {
	Capacity  uint64
	Head      uint64
	Tail      uint64
	Occupancy uint64
	Dropped   uint64
	Closed    bool
}

// New creates and initializes a new lock-free RingBuffer with the given capacity.
// Capacity MUST be a power of 2 greater than or equal to 2 (e.g. 2, 4, 1024, 65536).
func New(capacity uint64) (*RingBuffer, error) {
	if capacity < 2 || (capacity&(capacity-1)) != 0 {
		return nil, ErrInvalidCapacity
	}

	rb := &RingBuffer{
		capacity: capacity,
		mask:     capacity - 1,
		slots:    make([]Slot, capacity),
	}

	return rb, nil
}

// Capacity returns the total capacity of the ring buffer.
func (b *RingBuffer) Capacity() uint64 {
	return b.capacity
}

// IsClosed returns true if the ring buffer has been closed.
func (b *RingBuffer) IsClosed() bool {
	return atomic.LoadUint32(&b.closed) == 1
}

// Close marks the ring buffer as closed. Future Push operations will return ErrClosed.
func (b *RingBuffer) Close() error {
	atomic.StoreUint32(&b.closed, 1)
	return nil
}

// Push enqueues a TelemetryEvent into the ring buffer using overwrite-oldest semantics.
// If the buffer is full, the incoming event overwrites the oldest slot and advances the tail.
// It returns the assigned sequence ticket or an error if the buffer is closed.
func (b *RingBuffer) Push(event TelemetryEvent) (uint64, error) {
	if b.IsClosed() {
		return 0, ErrClosed
	}

	// Ensure the event is sealed with an integrity checksum before publishing
	if event.Checksum == 0 {
		event.Seal()
	}

	// Atomically claim the next global write ticket
	pos := atomic.AddUint64(&b.head, 1) - 1
	idx := pos & b.mask
	slot := &b.slots[idx]

	targetOdd := 2*pos + 1
	targetEven := 2*pos + 2

	// Spin until any previous writer for this slot (from a prior wrap-around) has committed.
	// Since writes are in-place memory copies taking a few nanoseconds, this is extremely fast.
	for {
		v := atomic.LoadUint64(&slot.version)
		if v >= targetOdd {
			// A future writer already overtook this slot (extreme pre-emption); do not overwrite.
			return pos, nil
		}
		// If the slot is even, it's quiescent and ready for this cycle.
		if v%2 == 0 {
			if atomic.CompareAndSwapUint64(&slot.version, v, targetOdd) {
				break
			}
		} else {
			runtime.Gosched()
		}
	}

	// Write event into slot
	slot.event = event

	// Publish write: transition version from odd to committed even
	atomic.StoreUint64(&slot.version, targetEven)

	return pos, nil
}

// PushMetric is a convenience helper for publishing a named numeric metric.
func (b *RingBuffer) PushMetric(producerID uint32, seq uint64, name string, val float64) (uint64, error) {
	return b.Push(NewMetricEvent(producerID, seq, name, val))
}

// PushLog is a convenience helper for publishing a log entry.
func (b *RingBuffer) PushLog(producerID uint32, seq uint64, level uint16, msg string) (uint64, error) {
	return b.Push(NewLogEvent(producerID, seq, level, msg))
}

// Pop dequeues the oldest available TelemetryEvent from the shared ring buffer.
// Returns (event, true) on success, or (TelemetryEvent{}, false) if no event is currently available.
func (b *RingBuffer) Pop() (TelemetryEvent, bool) {
	for {
		curHead := atomic.LoadUint64(&b.head)
		curTail := atomic.LoadUint64(&b.tail)

		// Check if empty
		if curTail >= curHead {
			return TelemetryEvent{}, false
		}

		// Check if consumers have been lapped by fast producers
		if curHead > curTail+b.capacity {
			newTail := curHead - b.capacity
			lapped := newTail - curTail
			if atomic.CompareAndSwapUint64(&b.tail, curTail, newTail) {
				atomic.AddUint64(&b.dropped, lapped)
			}
			// Retry after updating or failing CAS
			continue
		}

		// Attempt to claim the current tail slot
		if !atomic.CompareAndSwapUint64(&b.tail, curTail, curTail+1) {
			// Lost race with another consumer, retry
			continue
		}

		// Successfully claimed slot curTail
		idx := curTail & b.mask
		slot := &b.slots[idx]
		expectedEven := 2*curTail + 2

		// Wait briefly if producer is currently completing the write for this ticket
		var ev TelemetryEvent
		var valid bool

		for retries := 0; retries < 1000; retries++ {
			v1 := atomic.LoadUint64(&slot.version)
			if v1 > expectedEven {
				// Slot was already overwritten by a subsequent producer wrap-around!
				// Account for the drop and move on.
				break
			}
			if v1 == expectedEven {
				ev = slot.event
				v2 := atomic.LoadUint64(&slot.version)
				if v2 == expectedEven && ev.ValidateChecksum() {
					valid = true
					break
				}
				// If version changed during read or checksum failed, a producer started overwriting
				break
			}
			runtime.Gosched()
		}

		if valid {
			return ev, true
		}

		// The slot was lapped/overwritten or failed checksum during read; increment dropped and retry.
		atomic.AddUint64(&b.dropped, 1)
	}
}

// DrainBatchInto dequeues up to len(dst) available events into the provided slice.
// This allows zero heap allocations if the caller reuses a buffer.
// Returns the number of events drained.
func (b *RingBuffer) DrainBatchInto(dst []TelemetryEvent) int {
	n := 0
	max := len(dst)
	for n < max {
		ev, ok := b.Pop()
		if !ok {
			break
		}
		dst[n] = ev
		n++
	}
	return n
}

// DrainBatch drains up to maxBatch events from the ring buffer and returns them as a slice.
func (b *RingBuffer) DrainBatch(maxBatch int) []TelemetryEvent {
	if maxBatch <= 0 {
		return nil
	}
	res := make([]TelemetryEvent, 0, maxBatch)
	for i := 0; i < maxBatch; i++ {
		ev, ok := b.Pop()
		if !ok {
			break
		}
		res = append(res, ev)
	}
	return res
}

// DroppedCount returns the total number of events dropped due to buffer saturation/overwrites.
func (b *RingBuffer) DroppedCount() uint64 {
	return atomic.LoadUint64(&b.dropped)
}

// Stats returns an instantaneous snapshot of buffer metrics.
func (b *RingBuffer) Stats() BufferStats {
	head := atomic.LoadUint64(&b.head)
	tail := atomic.LoadUint64(&b.tail)
	dropped := atomic.LoadUint64(&b.dropped)

	var occupancy uint64
	if head > tail {
		diff := head - tail
		if diff > b.capacity {
			occupancy = b.capacity
		} else {
			occupancy = diff
		}
	}

	return BufferStats{
		Capacity:  b.capacity,
		Head:      head,
		Tail:      tail,
		Occupancy: occupancy,
		Dropped:   dropped,
		Closed:    b.IsClosed(),
	}
}
