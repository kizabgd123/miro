package telemetry

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"time"
)

// Event types
const (
	TypeUnknown uint16 = 0
	TypeMetric  uint16 = 1
	TypeLog     uint16 = 2
	TypeTrace   uint16 = 3
)

// MaxPayloadSize defines the static size of the embedded event payload.
const MaxPayloadSize = 48

// TelemetryEvent is a fixed-size, contiguous struct representing a telemetry record.
// Because it contains no pointers or slice headers, instances can be copied and stored
// directly in ring buffer slots with zero heap allocations.
type TelemetryEvent struct {
	Timestamp  int64              // Unix timestamp in nanoseconds
	ProducerID uint32             // ID of the producing process/goroutine
	Sequence   uint64             // Monotonic sequence number per producer
	Type       uint16             // Event type (Metric, Log, Trace)
	Value      float64            // Numeric metric value
	Payload    [MaxPayloadSize]byte // Inline buffer for metric name or log message
	PayloadLen uint8              // Active length of Payload
	Checksum   uint32             // CRC32 checksum over the fields above
}

// SetPayload copies up to MaxPayloadSize bytes into Payload and sets PayloadLen.
func (e *TelemetryEvent) SetPayload(data []byte) {
	n := len(data)
	if n > MaxPayloadSize {
		n = MaxPayloadSize
	}
	copy(e.Payload[:n], data[:n])
	e.PayloadLen = uint8(n)
}

// SetPayloadString copies a string up to MaxPayloadSize bytes into Payload.
func (e *TelemetryEvent) SetPayloadString(s string) {
	e.SetPayload([]byte(s))
}

// PayloadString returns the active payload as a string.
func (e *TelemetryEvent) PayloadString() string {
	return string(e.Payload[:e.PayloadLen])
}

// ComputeChecksum calculates the CRC32 IEEE checksum of the event's data fields.
// It formats bytes into a stack-allocated buffer to guarantee zero heap allocations.
func (e *TelemetryEvent) ComputeChecksum() uint32 {
	var buf [31 + MaxPayloadSize]byte // 8 + 4 + 8 + 2 + 8 + 48 + 1 = 79 bytes

	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.Timestamp))
	binary.LittleEndian.PutUint32(buf[8:12], e.ProducerID)
	binary.LittleEndian.PutUint64(buf[12:20], e.Sequence)
	binary.LittleEndian.PutUint16(buf[20:22], e.Type)
	binary.LittleEndian.PutUint64(buf[22:30], math.Float64bits(e.Value))
	copy(buf[30:30+MaxPayloadSize], e.Payload[:])
	buf[30+MaxPayloadSize] = e.PayloadLen

	return crc32.ChecksumIEEE(buf[:])
}

// Seal computes and sets the Checksum field.
func (e *TelemetryEvent) Seal() {
	e.Checksum = e.ComputeChecksum()
}

// ValidateChecksum verifies whether the Checksum matches the event's contents.
func (e *TelemetryEvent) ValidateChecksum() bool {
	return e.Checksum == e.ComputeChecksum()
}

// NewMetricEvent creates a sealed TelemetryEvent representing a numeric metric.
func NewMetricEvent(producerID uint32, seq uint64, name string, val float64) TelemetryEvent {
	e := TelemetryEvent{
		Timestamp:  time.Now().UnixNano(),
		ProducerID: producerID,
		Sequence:   seq,
		Type:       TypeMetric,
		Value:      val,
	}
	e.SetPayloadString(name)
	e.Seal()
	return e
}

// NewLogEvent creates a sealed TelemetryEvent representing a log record.
func NewLogEvent(producerID uint32, seq uint64, level uint16, msg string) TelemetryEvent {
	e := TelemetryEvent{
		Timestamp:  time.Now().UnixNano(),
		ProducerID: producerID,
		Sequence:   seq,
		Type:       TypeLog,
		Value:      float64(level),
	}
	e.SetPayloadString(msg)
	e.Seal()
	return e
}
