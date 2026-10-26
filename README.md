# Lock-Free Telemetry Ring Buffer

A high-performance, zero-allocation, lock-free Multi-Producer Multi-Consumer (MPMC) ring buffer in Go engineered for high-throughput streaming telemetry events.

## Features

- **Lock-Free Concurrency**: Operates entirely with atomic instructions (`sync/atomic`), avoiding mutexes, channel contention, and thread-level blocking.
- **Overwrite-Oldest Saturation**: When the buffer reaches capacity, incoming telemetry immediately claims slots and overwrites the oldest unconsumed events, prioritizing the freshest telemetry data.
- **Zero Torn Reads (Slot Seqlock & CRC32)**: Each slot contains an atomic version counter acting as a seqlock (`2*pos + 1` during write, `2*pos + 2` on publication) paired with CRC32 payload checksum verification. Readers detect concurrent overwrites and never see corrupted data.
- **Cache-Line Alignment**: Hot atomic variables (`head`, `tail`, `dropped`) are spaced with 64-byte padding to eliminate CPU false sharing and cache-line bouncing.
- **Zero Allocations in Hot Path**: Fixed-size contiguous struct layout (`TelemetryEvent`) prevents Go GC allocations on `Push`, `Pop`, and `DrainBatchInto`.
- **Exact Event Accounting**: Monotonically tracked metrics ensure $TotalProduced == TotalConsumed + TotalDropped$.

## Architecture

```
                    +------------------------------------+
                    |        Telemetry Ring Buffer       |
                    |           (Power-of-2 Cap)         |
                    +-----------------+------------------+
                                      |
       +-----------------------+------+-----------------------+
       |                       |                              |
  [Producer 1]            [Producer 2]                   [Producer N]
       |                       |                              |
       +-------> atomic.AddUint64(&head, 1) <-----------------+
                               |
                        [Slot Seqlock]
                 Odd = Writing | Even = Committed
                               |
       +-----------------------+-----------------------+
       |                                               |
  [Consumer 1]                                    [Consumer M]
       |                                               |
       +---------> atomic.CAS(&tail, cur, cur+1) <-----+
             (Auto fast-forward tail if lapped by head)
```

## Running Tests & Benchmarks

### Unit & Concurrency Tests
```bash
go test -v ./pkg/telemetry/...
```

### Benchmarks (Throughput & Zero-Allocation Check)
```bash
go test -bench=. -benchmem ./pkg/telemetry/...
```

### Standalone Stress Test CLI
```bash
go run ./cmd/telemetry-stress -producers 8 -consumers 4 -capacity 4096 -duration 3s
```

## Performance Benchmark Sample
- **Push**: ~34 ns/op (over 28 million pushes/sec, 0 allocs/op)
- **Contended Push/Pop**: ~80 ns/op (over 12 million concurrent ops/sec, 0 allocs/op)
- **Stress soak**: > 4.2 million events/sec under continuous overflow saturation with 0 torn reads.
