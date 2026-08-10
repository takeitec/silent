package main

import (
	"fmt"
	"strings"
	"time"
)

type streamHealthMetrics struct {
	TargetJitterDelay time.Duration
	ReceivedChunks    int64
	PlayedChunks      int64
	LateDropped       int64
	DuplicateDropped  int64
	Underflows        int64
	GapFillSilence    int64
	CatchupResyncs    int64
	MaxBufferedChunks int
	oneWaySamples     int64
	oneWayTotal       time.Duration
	oneWayMin         time.Duration
	oneWayMax         time.Duration
}

func newStreamHealthMetrics(targetDelay time.Duration) streamHealthMetrics {
	return streamHealthMetrics{TargetJitterDelay: targetDelay}
}

func (m *streamHealthMetrics) ObserveOneWay(delay time.Duration) {
	if delay < 0 {
		return
	}
	if m.oneWaySamples == 0 {
		m.oneWayMin = delay
		m.oneWayMax = delay
	} else {
		if delay < m.oneWayMin {
			m.oneWayMin = delay
		}
		if delay > m.oneWayMax {
			m.oneWayMax = delay
		}
	}
	m.oneWaySamples++
	m.oneWayTotal += delay
}

func (m streamHealthMetrics) OneWaySummary() string {
	parts := []string{fmt.Sprintf("target_jitter=%s", m.TargetJitterDelay)}
	if m.oneWaySamples == 0 {
		parts = append(parts, "samples=0")
		return strings.Join(parts, " ")
	}
	avg := m.oneWayTotal / time.Duration(m.oneWaySamples)
	parts = append(parts,
		fmt.Sprintf("samples=%d", m.oneWaySamples),
		fmt.Sprintf("avg=%s", avg),
		fmt.Sprintf("min=%s", m.oneWayMin),
		fmt.Sprintf("max=%s", m.oneWayMax),
	)
	return strings.Join(parts, " ")
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
