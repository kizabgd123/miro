package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"telemetryring/pkg/telemetry"
)

func main() {
	numProducers := flag.Int("producers", 8, "Number of concurrent producer goroutines")
	numConsumers := flag.Int("consumers", 4, "Number of concurrent consumer goroutines")
	capacity := flag.Uint64("capacity", 4096, "Ring buffer capacity (power of 2)")
	duration := flag.Duration("duration", 3*time.Second, "Soak duration for the stress test")
	batchSize := flag.Int("batch", 64, "Batch size for batch draining")
	flag.Parse()

	fmt.Println("=================================================================")
	fmt.Println("🚀 Lock-Free Telemetry Ring Buffer Stress Test")
	fmt.Println("=================================================================")
	fmt.Printf("Capacity:       %d slots\n", *capacity)
	fmt.Printf("Producers:      %d goroutines\n", *numProducers)
	fmt.Printf("Consumers:      %d goroutines\n", *numConsumers)
	fmt.Printf("Batch Size:     %d events\n", *batchSize)
	fmt.Printf("Duration:       %v\n", *duration)
	fmt.Println("-----------------------------------------------------------------")

	rb, err := telemetry.New(*capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating ring buffer: %v\n", err)
		os.Exit(1)
	}

	var totalProduced uint64
	var totalConsumed uint64
	var totalTornReads uint64
	var stopFlag int32

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup

	// Launch consumer goroutines
	for c := 0; c < *numConsumers; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			batch := make([]telemetry.TelemetryEvent, *batchSize)
			for atomic.LoadInt32(&stopFlag) == 0 {
				n := rb.DrainBatchInto(batch)
				if n > 0 {
					atomic.AddUint64(&totalConsumed, uint64(n))
					for i := 0; i < n; i++ {
						if !batch[i].ValidateChecksum() {
							atomic.AddUint64(&totalTornReads, 1)
						}
					}
				} else {
					time.Sleep(10 * time.Microsecond)
				}
			}
		}(c)
	}

	// Launch producer goroutines
	startTime := time.Now()
	for p := 0; p < *numProducers; p++ {
		wg.Add(1)
		go func(pid uint32) {
			defer wg.Done()
			seq := uint64(0)
			for atomic.LoadInt32(&stopFlag) == 0 {
				seq++
				val := float64(seq) * 1.618
				_, err := rb.PushMetric(pid, seq, "sensor.telemetry.temp", val)
				if err != nil {
					break
				}
				atomic.AddUint64(&totalProduced, 1)
			}
		}(uint32(p + 1))
	}

	// Monitor ticker
	ticker := time.NewTicker(500 * time.Millisecond)
	timer := time.NewTimer(*duration)
	defer ticker.Stop()
	defer timer.Stop()

	running := true
	for running {
		select {
		case <-timer.C:
			running = false
		case <-sigChan:
			fmt.Println("\nAborting on user interrupt...")
			running = false
		case <-ticker.C:
			elapsed := time.Since(startTime).Seconds()
			prod := atomic.LoadUint64(&totalProduced)
			cons := atomic.LoadUint64(&totalConsumed)
			drop := rb.DroppedCount()
			stats := rb.Stats()

			fmt.Printf("[%5.1fs] Produced: %10d | Consumed: %10d | Dropped: %10d | Occ: %5d/%d | Rate: %8.1f ev/s\n",
				elapsed, prod, cons, drop, stats.Occupancy, *capacity, float64(prod)/elapsed)
		}
	}

	atomic.StoreInt32(&stopFlag, 1)
	_ = rb.Close()
	wg.Wait()

	// Final drain
	finalBatch := make([]telemetry.TelemetryEvent, *batchSize)
	for {
		n := rb.DrainBatchInto(finalBatch)
		if n == 0 {
			break
		}
		atomic.AddUint64(&totalConsumed, uint64(n))
		for i := 0; i < n; i++ {
			if !finalBatch[i].ValidateChecksum() {
				atomic.AddUint64(&totalTornReads, 1)
			}
		}
	}

	totalTime := time.Since(startTime)
	prod := atomic.LoadUint64(&totalProduced)
	cons := atomic.LoadUint64(&totalConsumed)
	drop := rb.DroppedCount()
	torn := atomic.LoadUint64(&totalTornReads)

	fmt.Println("-----------------------------------------------------------------")
	fmt.Println("📊 Final Results:")
	fmt.Printf("Elapsed Time:   %v\n", totalTime)
	fmt.Printf("Total Produced: %d events (%.2f million/sec)\n", prod, float64(prod)/totalTime.Seconds()/1e6)
	fmt.Printf("Total Consumed: %d events (%.2f million/sec)\n", cons, float64(cons)/totalTime.Seconds()/1e6)
	fmt.Printf("Total Dropped:  %d events (overwritten)\n", drop)
	fmt.Printf("Torn Reads:     %d (corrupt events detected)\n", torn)
	fmt.Println("-----------------------------------------------------------------")

	if torn > 0 {
		fmt.Printf("❌ FAILED: Detected %d torn reads!\n", torn)
		os.Exit(1)
	}

	delta := int64(prod) - int64(cons+drop)
	if delta != 0 {
		fmt.Printf("❌ FAILED: Conservation violation: %d produced vs %d accounted for (delta %d)\n",
			prod, cons+drop, delta)
		os.Exit(1)
	}

	fmt.Println("✅ PASSED: Exact event conservation & ZERO torn reads verified!")
}
