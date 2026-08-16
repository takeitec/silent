package main

import (
	"fmt"
	"strings"
	"time"
)

var delayHistogramBounds = []time.Duration{
	1 * time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	30 * time.Millisecond,
	40 * time.Millisecond,
	50 * time.Millisecond,
	75 * time.Millisecond,
	100 * time.Millisecond,
	150 * time.Millisecond,
	200 * time.Millisecond,
	300 * time.Millisecond,
	400 * time.Millisecond,
	500 * time.Millisecond,
	750 * time.Millisecond,
	1 * time.Second,
	1500 * time.Millisecond,
	2 * time.Second,
	3 * time.Second,
	5 * time.Second,
}

type delaySummary struct {
	samples int64
	total   time.Duration
	min     time.Duration
	max     time.Duration
	buckets []int64
}

func newDelaySummary() delaySummary {
	return delaySummary{min: -1, buckets: make([]int64, len(delayHistogramBounds)+1)}
}

func (d *delaySummary) Observe(delay time.Duration) {
	if delay < 0 {
		return
	}
	if d.samples == 0 {
		d.min = delay
		d.max = delay
	} else {
		if delay < d.min {
			d.min = delay
		}
		if delay > d.max {
			d.max = delay
		}
	}
	d.samples++
	d.total += delay
	for idx, bound := range delayHistogramBounds {
		if delay <= bound {
			d.buckets[idx]++
			return
		}
	}
	d.buckets[len(d.buckets)-1]++
}

func (d delaySummary) percentile(numerator, denominator int64) time.Duration {
	if d.samples == 0 {
		return 0
	}
	rank := (d.samples*numerator + denominator - 1) / denominator
	seen := int64(0)
	for idx, count := range d.buckets {
		seen += count
		if seen >= rank {
			if idx < len(delayHistogramBounds) {
				return delayHistogramBounds[idx]
			}
			return delayHistogramBounds[len(delayHistogramBounds)-1]
		}
	}
	return d.max
}

func (d delaySummary) Summary() string {
	if d.samples == 0 {
		return "samples=0"
	}
	avg := d.total / time.Duration(d.samples)
	return fmt.Sprintf("samples=%d avg=%s min=%s p50=%s p90=%s p95=%s p99=%s max=%s",
		d.samples,
		avg,
		d.min,
		d.percentile(50, 100),
		d.percentile(90, 100),
		d.percentile(95, 100),
		d.percentile(99, 100),
		d.max,
	)
}

type streamHealthMetrics struct {
	TargetJitterDelay     time.Duration
	ReceivedChunks        int64
	PlayoutEnqueuedChunks int64
	SinkWrittenChunks     int64
	LateDropped           int64
	DuplicateDropped      int64
	Underflows            int64
	GapFillSilence        int64
	CatchupResyncs        int64
	HardResyncs           int64
	SinkQueueDropped      int64
	DecodedChunks         int64
	DecodeTotal           time.Duration
	DecodeMax             time.Duration
	DecodeErrors          int64
	MaxBufferedChunks     int
	oneWay                delaySummary
	producedToRecv        delaySummary
	recvToScheduled       delaySummary
	scheduledToWrite      delaySummary
	producedToSched       delaySummary
	producedToWrite       delaySummary
	sinkWrite             delaySummary
	sendBlock             delaySummary
}

func newStreamHealthMetrics(targetDelay time.Duration) streamHealthMetrics {
	return streamHealthMetrics{
		TargetJitterDelay: targetDelay,
		oneWay:            newDelaySummary(),
		producedToRecv:    newDelaySummary(),
		recvToScheduled:   newDelaySummary(),
		scheduledToWrite:  newDelaySummary(),
		producedToSched:   newDelaySummary(),
		producedToWrite:   newDelaySummary(),
		sinkWrite:         newDelaySummary(),
		sendBlock:         newDelaySummary(),
	}
}

func (m *streamHealthMetrics) ObserveOneWay(delay time.Duration) {
	m.oneWay.Observe(delay)
}

func (m streamHealthMetrics) OneWaySummary() string {
	parts := []string{fmt.Sprintf("target_jitter=%s", m.TargetJitterDelay), m.oneWay.Summary()}
	return strings.Join(parts, " ")
}

func (m *streamHealthMetrics) ObserveProducedToRecv(delay time.Duration) {
	m.producedToRecv.Observe(delay)
}

func (m streamHealthMetrics) ProducedToRecvSummary() string {
	return m.producedToRecv.Summary()
}

func (m *streamHealthMetrics) ObserveRecvToScheduled(delay time.Duration) {
	m.recvToScheduled.Observe(delay)
}

func (m streamHealthMetrics) RecvToScheduledSummary() string {
	return m.recvToScheduled.Summary()
}

func (m *streamHealthMetrics) ObserveScheduledToWrite(delay time.Duration) {
	m.scheduledToWrite.Observe(delay)
}

func (m streamHealthMetrics) ScheduledToWriteSummary() string {
	return m.scheduledToWrite.Summary()
}

func (m *streamHealthMetrics) ObserveProducedToScheduled(delay time.Duration) {
	m.producedToSched.Observe(delay)
}

func (m streamHealthMetrics) ProducedToScheduledSummary() string {
	return m.producedToSched.Summary()
}

func (m *streamHealthMetrics) ObserveProducedToWrite(delay time.Duration) {
	m.producedToWrite.Observe(delay)
}

func (m streamHealthMetrics) ProducedToWriteSummary() string {
	return m.producedToWrite.Summary()
}

func (m *streamHealthMetrics) ObserveSinkWrite(delay time.Duration) {
	m.sinkWrite.Observe(delay)
}

func (m streamHealthMetrics) SinkWriteSummary() string {
	return m.sinkWrite.Summary()
}

func (m streamHealthMetrics) DecodeSummary() string {
	if m.DecodedChunks == 0 {
		return "samples=0"
	}
	return fmt.Sprintf("samples=%d avg=%s max=%s errors=%d", m.DecodedChunks, m.DecodeTotal/time.Duration(m.DecodedChunks), m.DecodeMax, m.DecodeErrors)
}

func (m *streamHealthMetrics) ObserveDecode(duration time.Duration) {
	m.DecodedChunks++
	m.DecodeTotal += duration
	if duration > m.DecodeMax {
		m.DecodeMax = duration
	}
}

func (m *streamHealthMetrics) ObserveSendBlock(delay time.Duration) {
	m.sendBlock.Observe(delay)
}

func (m streamHealthMetrics) SendBlockSummary() string {
	return m.sendBlock.Summary()
}

func streamChunkDuration(format streamFormat) time.Duration {
	bytesPerSample := sampleBytesPerChannel(format.SampleFormat)
	bytesPerSecond := format.SampleRate * format.Channels * bytesPerSample
	if bytesPerSecond <= 0 || format.ChunkBytes <= 0 {
		return 20 * time.Millisecond
	}
	seconds := float64(format.ChunkBytes) / float64(bytesPerSecond)
	dur := time.Duration(seconds * float64(time.Second))
	if dur <= 0 {
		return 20 * time.Millisecond
	}
	return dur
}

func sampleBytesPerChannel(sampleFormat string) int {
	switch strings.ToLower(strings.TrimSpace(sampleFormat)) {
	case "u8":
		return 1
	case "s16", "s16le", "s16be":
		return 2
	case "s24", "s24le", "s24be":
		return 3
	case "s32", "s32le", "s32be", "flt", "fltp", "f32le", "f32be":
		return 4
	case "dbl", "dblp", "f64le", "f64be":
		return 8
	default:
		return 2
	}
}
