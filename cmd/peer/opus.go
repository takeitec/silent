package main

import (
	"strings"
	"time"
)

type payloadCodec string

type opusImplementation string

const (
	payloadCodecPCM  payloadCodec = "pcm"
	payloadCodecOpus payloadCodec = "opus"

	opusImplementationHraban opusImplementation = "hraban"
	opusImplementationPion   opusImplementation = "pion"

	opusFrameDuration  = 20 * time.Millisecond
	opus128kbpsBitrate = 128000
	opus320kbpsBitrate = 320000

	opusMaxPacketBytes = 4000
)

type audioEncoder interface {
	EncodePCM(pcm []byte) ([]byte, error)
	Close() error
}

type audioDecoder interface {
	DecodePacket(packet []byte) ([]byte, error)
	// Conceal synthesizes a plausible frame for a packet that never
	// arrived, using the decoder's own packet-loss concealment rather
	// than inserting silence. Must be called in the same chronological
	// (sequence) order the real packets would have been decoded in, or
	// the concealment will not reflect the decoder's true internal state.
	Conceal() ([]byte, error)
	// Reset clears the decoder's internal predictive state. Call this on
	// a known, deliberate discontinuity (e.g. the leader fast-forwarding
	// a lagging subscriber past evicted ring-buffer chunks) rather than
	// Conceal - concealment is designed to paper over a handful of
	// missing frames and assumes the audio on either side is continuous;
	// it is the wrong tool for a large intentional jump to an unrelated
	// point in the stream, and repeated concealment across a jump like
	// that tends to degrade toward silence or a hum rather than
	// producing anything useful. A reset instead lets the next real
	// packet decode cleanly, as if starting a fresh stream, at the cost
	// of a single frame's worth of lost overlap-add continuity - a minor,
	// one-off artifact instead of a stretch of synthesized nonsense.
	Reset() error
	Close() error
}

func normaliseStreamCodec(value string) payloadCodec {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "opus":
		return payloadCodecOpus
	case "pcm":
		return payloadCodecPCM
	default:
		return payloadCodecPCM
	}
}

func normaliseOpusImplementation(value string) opusImplementation {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "pion":
		return opusImplementationPion
	case "hraban":
		fallthrough
	default:
		return opusImplementationHraban
	}
}

func newOpusEncoder(implementation opusImplementation, sampleRate int, channels int, bitrate int) (audioEncoder, error) {
	switch implementation {
	case opusImplementationPion:
		return newPionOpusEncoder(sampleRate, channels, bitrate)
	case opusImplementationHraban:
		fallthrough
	default:
		return newHrabanOpusEncoder(sampleRate, channels, bitrate)
	}
}

func newOpusDecoder(implementation opusImplementation, sampleRate int, channels int) (audioDecoder, error) {
	switch implementation {
	case opusImplementationPion:
		return newPionOpusDecoder(sampleRate, channels)
	case opusImplementationHraban:
		fallthrough
	default:
		return newHrabanOpusDecoder(sampleRate, channels)
	}
}
