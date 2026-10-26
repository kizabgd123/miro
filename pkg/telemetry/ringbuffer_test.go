package telemetry

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPowerOfTwoValidation(t *testing.T) {
	validCaps := []uint64{2, 4, 8, 16, 64, 1024, 65536}
	for _, cap := range validCaps {
		rb, err := New(cap)
		if err != nil {
			t.Fatalf("expected valid capacity %d, got error: %v", cap, err)
		}
		if rb.Capacity() != cap {
			t.Fatalf("expected capacity %d, got %d", cap, rb.Capacity())
		}
	}

	invalidCaps := []uint64{0, 1, 3, 5, 7, 10, 100, 1000}
	for _, cap := range invalidCaps {
		_, err := New(cap)
		if err != ErrInvalidCapacity {
			t.Fatalf("expected ErrInvalidCapacity for cap %d, got %v", cap, err)
		}
	}
}

func TestSingleThreadedPushPop(t *testing.T) {
	rb, err := New(8)
	if err != nil {
		t.Fatalf("failed to create ring buffer: %v", err)
	}

	// Buffer starts empty
	if _, ok := rb.Pop(); ok {
		t.Fatal("expected empty buffer")
	}

	for i := uint64(0); i < 4; i++ {
		_, err := rb.PushMetric(1, i, fmt.Sprintf("cpu.load.%d", i), float64(i)*1.5)
		if err != nil {
			t.Fatalf("failed to push: %v", err)
		}
	}

	stats := rb.Stats()
	if stats.Occupancy != 4 {
		t.Fatalf("expected occupancy 4, got %d", stats.Occupancy)
	}

	for i := uint64(0); i < 4; i++ {
		ev, ok := rb.Pop()
		if !ok {
			t.Fatalf("expected event %d", i)
		}
		if !ev.ValidateChecksum() {
			t.Fatalf("event %d failed checksum validation", i)
		}
		if ev.Sequence != i {
			t.Fatalf("expected sequence %d, got %d", i, ev.Sequence)
		}
		expectedName := fmt.Sprintf("cpu.load.%d", i)
		if ev.PayloadString() != expectedName {
			t.Fatalf("expected payload %s, got %s", expectedName, ev.PayloadString())
		}
		if ev.Value != float64(i)*1.5 {
			t.Fatalf("expected value %f, got %f", float64(i)*1.5, ev.Value)
		}
	}

	// Buffer should now be empty
	if _, ok := rb.Pop(); ok {
		t.Fatal("expected empty buffer after popping all events")
	}
}

func TestCloseBehavior(t *testing.T) {
	rb, err := New(8)
	if err != nil {
		t.Fatalf("failed to create ring buffer: %v", err)
	}

	rb.PushMetric(1, 100, "mem.used", 42.0)
	if err := rb.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	if !rb.IsClosed() {
		t.Fatal("expected IsClosed to be true")
	}

	// Future pushes should be rejected
	if _, err := rb.PushMetric(1, 101, "mem.used", 43.0); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}

	// Existing items can still be drained
	ev, ok := rb.Pop()
	if !ok || ev.Sequence != 100 {
		t.Fatalf("expected to pop pre-close event, got ok=%v, seq=%d", ok, ev.Sequence)
	}

	// Subsequent pop on empty closed buffer returns false
	if _, ok := rb.Pop(); ok {
		t.Fatal("expected false on empty closed buffer")
	}
}

func TestOverwriteOldestSingleThread(t *testing.T) {
	// Ring buffer of size 4
	rb, err := New(4)
	if err != nil {
		t.Fatalf("failed to create ring buffer: %v", err)
	}

	// Push 10 events (0..9)
	for i := uint64(0); i < 10; i++ {
		_, err := rb.PushMetric(1, i, "metric", float64(i))
		if err != nil {
			t.Fatalf("push failed: %v", err)
		}
	}

	// Buffer capacity is 4. Events 0..5 should have been overwritten (6 dropped).
	// The 4 remaining events in the buffer should be 6, 7, 8, 9.
	var popped []uint64
	for {
		ev, ok := rb.Pop()
		if !ok {
			break
		}
		if !ev.ValidateChecksum() {
			t.Fatalf("popped event failed checksum: %+v", ev)
		}
		popped = append(popped, ev.Sequence)
	}

	if len(popped) != 4 {
		t.Fatalf("expected 4 remaining events, got %d: %v", len(popped), popped)
	}

	for i, expected := range []uint64{6, 7, 8, 9} {
		if popped[i] != expected {
			t.Fatalf("at index %d: expected seq %d, got %d", i, expected, popped[i])
		}
	}

	if rb.DroppedCount() != 6 {
		t.Fatalf("expected 6 dropped events, got %d", rb.DroppedCount())
	}
}

func TestBatchDrain(t *testing.T) {
	rb, _ := New(16)
	for i := uint64(0); i < 10; i++ {
		rb.PushLog(1, i, 1, fmt.Sprintf("log-%d", i))
	}

	// Drain into reusable slice
	buf := make([]TelemetryEvent, 6)
	drained := rb.DrainBatchInto(buf)
	if drained != 6 {
		t.Fatalf("expected 6 drained events, got %d", drained)
	}

	for i := 0; i < 6; i++ {
		if buf[i].Sequence != uint64(i) {
			t.Fatalf("expected seq %d, got %d", i, buf[i].Sequence)
		}
	}

	// Drain remaining with DrainBatch
	remaining := rb.DrainBatch(10)
	if len(remaining) != 4 {
		t.Fatalf("expected 4 remaining events, got %d", len(remaining))
	}
	for i, ev := range remaining {
		if ev.Sequence != uint64(i+6) {
			t.Fatalf("expected seq %d, got %d", i+6, ev.Sequence)
		}
	}
}

// TestZeroTornReads runs high-concurrency producers and consumers on a small buffer,
// forcing continuous overwrites and verifying that EVERY consumed event has a valid checksum.
func TestZeroTornReads(t *testing.T) {
	const capacity = 64
	const numProducers = 16
	const numConsumers = 8
	const duration = 2 * time.Second

	rb, err := New(capacity)
	if err != nil {
		t.Fatalf("failed to create ring buffer: %v", err)
	}

	var totalConsumed uint64
	var tornReads uint64
	stop := int32(0)

	var wg sync.WaitGroup

	// Start consumers
	for c := 0; c < numConsumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt32(&stop) == 0 {
				ev, ok := rb.Pop()
				if ok {
					atomic.AddUint64(&totalConsumed, 1)
					if !ev.ValidateChecksum() {
						atomic.AddUint64(&tornReads, 1)
					}
				} else {
					time.Sleep(10 * time.Microsecond)
				}
			}
		}()
	}

	// Start producers
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(prodID uint32) {
			defer wg.Done()
			seq := uint64(0)
			for atomic.LoadInt32(&stop) == 0 {
				seq++
				_, _ = rb.PushMetric(prodID, seq, fmt.Sprintf("prod.%d.metric", prodID), float64(seq)*3.14)
			}
		}(uint32(p))
	}

	time.Sleep(duration)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()

	t.Logf("TestZeroTornReads: total consumed = %d, torn reads = %d, dropped = %d",
		atomic.LoadUint64(&totalConsumed), atomic.LoadUint64(&tornReads), rb.DroppedCount())

	if atomic.LoadUint64(&tornReads) > 0 {
		t.Fatalf("FAIL: Detected %d torn reads / corrupt events!", atomic.LoadUint64(&tornReads))
	}
}

// TestEventConservation tests the fundamental invariant under heavy concurrent load:
// Total Produced == Total Consumed + Total Dropped
func TestEventConservation(t *testing.T) {
	const capacity = 128
	const numProducers = 8
	const eventsPerProducer = 10000
	const numConsumers = 4

	rb, err := New(capacity)
	if err != nil {
		t.Fatalf("failed to create ring buffer: %v", err)
	}

	var totalConsumed uint64
	var tornReads uint64
	var producersDone int32

	var wg sync.WaitGroup

	// Start consumers
	for c := 0; c < numConsumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ev, ok := rb.Pop()
				if ok {
					atomic.AddUint64(&totalConsumed, 1)
					if !ev.ValidateChecksum() {
						atomic.AddUint64(&tornReads, 1)
					}
				} else {
					if atomic.LoadInt32(&producersDone) == 1 {
						// Double check if empty
						if _, ok2 := rb.Pop(); !ok2 {
							break
						}
					}
					time.Sleep(20 * time.Microsecond)
				}
			}
		}()
	}

	// Start producers
	var prodWg sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		prodWg.Add(1)
		go func(prodID uint32) {
			defer prodWg.Done()
			for seq := uint64(1); seq <= eventsPerProducer; seq++ {
				_, _ = rb.PushLog(prodID, seq, 1, fmt.Sprintf("event-%d-%d", prodID, seq))
			}
		}(uint32(p))
	}

	prodWg.Wait()
	atomic.StoreInt32(&producersDone, 1)
	wg.Wait()

	// Drain any remaining events in the ring buffer
	for {
		ev, ok := rb.Pop()
		if !ok {
			break
		}
		atomic.AddUint64(&totalConsumed, 1)
		if !ev.ValidateChecksum() {
			atomic.AddUint64(&tornReads, 1)
		}
	}

	totalProduced := uint64(numProducers * eventsPerProducer)
	consumed := atomic.LoadUint64(&totalConsumed)
	dropped := rb.DroppedCount()

	t.Logf("Event Conservation Check: Produced=%d, Consumed=%d, Dropped=%d (Sum=%d)",
		totalProduced, consumed, dropped, consumed+dropped)

	if tornReads > 0 {
		t.Fatalf("FAIL: %d torn reads detected", tornReads)
	}

	if consumed+dropped != totalProduced {
		t.Fatalf("FAIL: Conservation invariant violated! Produced (%d) != Consumed (%d) + Dropped (%d) [Delta: %d]",
			totalProduced, consumed, dropped, int64(totalProduced)-int64(consumed+dropped))
	}
}

func BenchmarkPush(b *testing.B) {
	rb, _ := New(65536)
	ev := NewMetricEvent(1, 1, "cpu.temp", 65.5)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = rb.Push(ev)
	}
}

func BenchmarkPop(b *testing.B) {
	rb, _ := New(65536)
	ev := NewMetricEvent(1, 1, "cpu.temp", 65.5)
	for i := 0; i < 65536; i++ {
		_, _ = rb.Push(ev)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, ok := rb.Pop()
		if !ok {
			b.StopTimer()
			for j := 0; j < 65536; j++ {
				_, _ = rb.Push(ev)
			}
			b.StartTimer()
		}
	}
}

func BenchmarkContendedPushPop(b *testing.B) {
	rb, _ := New(65536)
	ev := NewMetricEvent(1, 1, "network.bytes", 1024.0)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				_, _ = rb.Push(ev)
			} else {
				_, _ = rb.Pop()
			}
			i++
		}
	})
}
