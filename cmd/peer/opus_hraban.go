package main

import (
	"fmt"
	"time"

	"github.com/hraban/opus"
)

type HrabanOpusEncoder struct {
	encoder       *opus.Encoder
	sampleRate    int
	channels      int
	frameSamples  int // per channel: 960 for 20 ms at 48 kHz
	framePCMBytes int // 3840 for stereo s16le

	// Reused across calls to avoid a heap allocation on every 20ms frame.
	sampleBuf []int16
	outBuf    []byte
}

type HrabanOpusDecoder struct {
	decoder       *opus.Decoder
	sampleRate    int
	channels      int
	frameSamples  int // per channel: 960 for 20 ms at 48 kHz
	framePCMBytes int // 3840 for stereo s16le

	// Reused across calls to avoid a heap allocation on every 20ms frame.
	sampleBuf []int16
	outBuf    []byte
}

func newHrabanOpusEncoder(sampleRate int, channels int, bitrate int) (*HrabanOpusEncoder, error) {
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
	return &HrabanOpusEncoder{
		encoder:       encoder,
		sampleRate:    sampleRate,
		channels:      channels,
		frameSamples:  frameSamples,
		framePCMBytes: framePCMBytes,
		sampleBuf:     make([]int16, frameSamples*channels),
		outBuf:        make([]byte, opusMaxPacketBytes),
	}, nil
}

func (oe *HrabanOpusEncoder) EncodePCM(pcm []byte) ([]byte, error) {
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

func (oe *HrabanOpusEncoder) Close() error {
	// No resources to release for the Opus encoder in this implementation
	return nil
}

func newHrabanOpusDecoder(sampleRate int, channels int) (*HrabanOpusDecoder, error) {
	decoder, err := opus.NewDecoder(sampleRate, channels)
	if err != nil {
		return nil, err
	}
	frameSamples := sampleRate * int(opusFrameDuration) / int(time.Second)
	framePCMBytes := frameSamples * channels * 2
	return &HrabanOpusDecoder{
		decoder:       decoder,
		sampleRate:    sampleRate,
		channels:      channels,
		frameSamples:  frameSamples,
		framePCMBytes: framePCMBytes,
		sampleBuf:     make([]int16, frameSamples*channels),
		outBuf:        make([]byte, opusMaxPacketBytes),
	}, nil
}

func (od *HrabanOpusDecoder) DecodeInto(packet []byte) ([]byte, error) {
	decodedSamples, err := od.decoder.Decode(packet, od.sampleBuf)
	if err != nil {
		return nil, err
	}

	// The caller is expected to copy this before the next Decode/Conceal
	// call, since the backing array is reused.
	for index := 0; index < decodedSamples*od.channels; index++ {
		od.outBuf[index*2] = byte(od.sampleBuf[index])
		od.outBuf[index*2+1] = byte(od.sampleBuf[index] >> 8)
	}

	return od.outBuf[:decodedSamples*od.channels*2], nil
}

func (od *HrabanOpusDecoder) DecodePacket(packet []byte) ([]byte, error) {
	if len(packet) == 0 {
		return nil, fmt.Errorf("decode Opus packet: empty packet (use Conceal for lost frames)")
	}

	pcm, err := od.DecodeInto(packet)
	if err != nil {
		return nil, fmt.Errorf("decode Opus packet: %w", err)
	}
	return pcm, nil
}

// Conceal asks the decoder to synthesize the next frame using the library's
// packet-loss concealment API, rather than callers inserting raw silence.
// hraban/opus exposes this explicitly as DecodePLC, and passing a nil packet
// is rejected as "no data supplied".
func (od *HrabanOpusDecoder) Conceal() ([]byte, error) {
	if err := od.decoder.DecodePLC(od.sampleBuf); err != nil {
		return nil, fmt.Errorf("conceal Opus frame: %w", err)
	}

	// The caller is expected to copy this before the next Decode/Conceal
	// call, since the backing array is reused.
	for index := 0; index < od.frameSamples*od.channels; index++ {
		od.outBuf[index*2] = byte(od.sampleBuf[index])
		od.outBuf[index*2+1] = byte(od.sampleBuf[index] >> 8)
	}
	return od.outBuf[:od.frameSamples*od.channels*2], nil
}

func (od *HrabanOpusDecoder) Close() error {
	// No resources to release for the Opus decoder in this implementation
	return nil
}

func (od *HrabanOpusDecoder) Reset() error {
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
