package main

import (
	"fmt"
	"time"

	"github.com/hraban/opus"
)

type payloadCodec string

const (
	payloadCodecPCM  payloadCodec = "pcm"
	payloadCodecOpus payloadCodec = "opus"

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

type OpusEncoder struct {
	encoder       *opus.Encoder
	sampleRate    int
	channels      int
	frameSamples  int // per channel: 960 for 20 ms at 48 kHz
	framePCMBytes int // 3840 for stereo s16le

	// Reused across calls to avoid a heap allocation on every 20ms frame.
	sampleBuf []int16
	outBuf    []byte
}

type OpusDecoder struct {
	decoder       *opus.Decoder
	sampleRate    int
	channels      int
	frameSamples  int // per channel: 960 for 20 ms at 48 kHz
	framePCMBytes int // 3840 for stereo s16le

	// Reused across calls to avoid a heap allocation on every 20ms frame.
	sampleBuf []int16
}

func newOpusEncoder(sampleRate int, channels int, bitrate int) (*OpusEncoder, error) {
	encoder, err := opus.NewEncoder(sampleRate, channels, opus.Application(opus.AppAudio))
	if err != nil {
		return nil, err
	}
	if bitrate <= 0 {
		bitrate = opus128kbpsBitrate
	}
	if err := encoder.SetBitrate(bitrate); err != nil {
		return nil, fmt.Errorf("set Opus bitrate: %w", err)
	}
	frameSamples := sampleRate * int(opusFrameDuration) / int(time.Second)
	framePCMBytes := frameSamples * channels * 2
	return &OpusEncoder{
		encoder:       encoder,
		sampleRate:    sampleRate,
		channels:      channels,
		frameSamples:  frameSamples,
		framePCMBytes: framePCMBytes,
		sampleBuf:     make([]int16, frameSamples*channels),
		outBuf:        make([]byte, opusMaxPacketBytes),
	}, nil
}

func (oe *OpusEncoder) EncodePCM(pcm []byte) ([]byte, error) {
	// Validate PCM length
	if len(pcm) != oe.framePCMBytes {
		return nil, fmt.Errorf(
			"invalid Opus PCM frame: got=%d bytes want=%d bytes sample_rate=%d channels=%d frame_samples=%d",
			len(pcm),
			oe.framePCMBytes,
			oe.sampleRate,
			oe.channels,
			oe.frameSamples,
		)
	}

	// Convert PCM bytes to int16 samples
	for index := 0; index < oe.frameSamples*oe.channels; index++ {
		oe.sampleBuf[index] = int16(pcm[index*2]) | int16(pcm[index*2+1])<<8
	}

	// Encode the samples to Opus. The caller is expected to copy the
	// result (e.g. via append([]byte(nil), ...)) before the next call,
	// since oe.outBuf is reused.
	encodedBytes, err := oe.encoder.Encode(oe.sampleBuf, oe.outBuf)
	if err != nil {
		return nil, fmt.Errorf("encode Opus frame: %w", err)
	}

	return oe.outBuf[:encodedBytes], nil
}

func (oe *OpusEncoder) Close() error {
	// No resources to release for the Opus encoder in this implementation
	return nil
}

func newOpusDecoder(sampleRate int, channels int) (*OpusDecoder, error) {
	decoder, err := opus.NewDecoder(sampleRate, channels)
	if err != nil {
		return nil, err
	}
	frameSamples := sampleRate * int(opusFrameDuration) / int(time.Second)
	framePCMBytes := frameSamples * channels * 2
	return &OpusDecoder{
		decoder:       decoder,
		sampleRate:    sampleRate,
		channels:      channels,
		frameSamples:  frameSamples,
		framePCMBytes: framePCMBytes,
		sampleBuf:     make([]int16, frameSamples*channels),
	}, nil
}

func (od *OpusDecoder) DecodeInto(packet []byte) ([]byte, error) {
	decodedSamples, err := od.decoder.Decode(packet, od.sampleBuf)
	if err != nil {
		return nil, err
	}

	// The caller is expected to copy this before the next Decode/Conceal
	// call, since the backing array is reused.
	pcm := make([]byte, decodedSamples*od.channels*2)
	for index := 0; index < decodedSamples*od.channels; index++ {
		pcm[index*2] = byte(od.sampleBuf[index])
		pcm[index*2+1] = byte(od.sampleBuf[index] >> 8)
	}

	return pcm, nil
}

func (od *OpusDecoder) DecodePacket(packet []byte) ([]byte, error) {
	if len(packet) == 0 {
		return nil, fmt.Errorf("decode Opus packet: empty packet (use Conceal for lost frames)")
	}

	pcm, err := od.DecodeInto(packet)
	if err != nil {
		return nil, fmt.Errorf("decode Opus packet: %w", err)
	}
	return pcm, nil
}

// Conceal asks the decoder to synthesize the next frame using its own
// packet-loss concealment (equivalent to libopus's opus_decode with a NULL
// packet), rather than callers inserting raw silence. This keeps the
// decoder's internal predictive state consistent with elapsed time, so the
// next real packet decodes cleanly instead of picking up mid-gap.
func (od *OpusDecoder) Conceal() ([]byte, error) {
	pcm, err := od.DecodeInto(nil)
	if err != nil {
		return nil, fmt.Errorf("conceal Opus frame: %w", err)
	}
	return pcm, nil
}

func (od *OpusDecoder) Close() error {
	// No resources to release for the Opus decoder in this implementation
	return nil
}

func (od *OpusDecoder) Reset() error {
	// Recreate the underlying decoder rather than relying on an in-place
	// reset method, since this is cheap (no allocation on the hot path -
	// it only runs on a discontinuity, not per-chunk) and works
	// regardless of whether the wrapped library exposes a native reset.
	decoder, err := opus.NewDecoder(od.sampleRate, od.channels)
	if err != nil {
		return fmt.Errorf("reset Opus decoder: %w", err)
	}
	od.decoder = decoder
	return nil
}
