package main

import (
	"fmt"
	"time"

	pionopus "github.com/pion/opus"
)

// Paired with PionOpusDecoder, this gives a fully pure-Go Opus pipeline
// with no cgo/libopus dependency, as an alternative to hraban/opus.
type PionOpusEncoder struct {
	encoder       *pionopus.Encoder
	channels      int
	frameSamples  int
	framePCMBytes int

	// Reused across calls to avoid a heap allocation on every 20ms frame.
	outBuf []byte
}

func newPionOpusEncoder(sampleRate int, channels int, bitrate int) (*PionOpusEncoder, error) {
	if bitrate <= 0 {
		bitrate = opus128kbpsBitrate
	}
	encoder, err := pionopus.NewEncoder(
		pionopus.WithSampleRate(sampleRate),
		pionopus.WithChannels(channels),
		pionopus.WithBitrate(bitrate),
		pionopus.WithApplication(pionopus.ApplicationAudio),
	)
	if err != nil {
		return nil, fmt.Errorf("create pion Opus encoder: %w", err)
	}
	frameSamples := sampleRate * int(opusFrameDuration) / int(time.Second)
	framePCMBytes := frameSamples * channels * 2
	return &PionOpusEncoder{
		encoder:       encoder,
		channels:      channels,
		frameSamples:  frameSamples,
		framePCMBytes: framePCMBytes,
		outBuf:        make([]byte, opusMaxPacketBytes),
	}, nil
}

func (pe *PionOpusEncoder) EncodePCM(pcm []byte) ([]byte, error) {
	if len(pcm) != pe.framePCMBytes {
		return nil, fmt.Errorf(
			"invalid Opus PCM frame (pion): got=%d bytes want=%d bytes channels=%d frame_samples=%d",
			len(pcm), pe.framePCMBytes, pe.channels, pe.frameSamples,
		)
	}

	// The caller is expected to copy this before the next Decode/Conceal
	// call, since the backing array is reused.
	// pion's Encode takes S16LE PCM bytes directly, no manual int16
	// conversion needed (unlike hraban's wrapper).
	n, err := pe.encoder.Encode(pcm, pe.outBuf)
	if err != nil {
		return nil, fmt.Errorf("encode Opus frame (pion): %w", err)
	}
	return pe.outBuf[:n], nil
}

func (pe *PionOpusEncoder) Close() error {
	return nil
}

type PionOpusDecoder struct {
	decoder      pionopus.Decoder
	sampleRate   int
	channels     int
	frameSamples int

	// Reused across calls to avoid a heap allocation on every 20ms frame.
	sampleBuf []int16
	outBuf    []byte
}

func newPionOpusDecoder(sampleRate int, channels int) (*PionOpusDecoder, error) {
	decoder, err := pionopus.NewDecoderWithOutput(sampleRate, channels)
	if err != nil {
		return nil, fmt.Errorf("create pion Opus decoder: %w", err)
	}
	frameSamples := sampleRate * int(opusFrameDuration) / int(time.Second)
	return &PionOpusDecoder{
		decoder:      decoder,
		sampleRate:   sampleRate,
		channels:     channels,
		frameSamples: frameSamples,
		sampleBuf:    make([]int16, frameSamples*channels),
		outBuf:       make([]byte, opusMaxPacketBytes),
	}, nil
}

func (pd *PionOpusDecoder) DecodePacket(packet []byte) ([]byte, error) {
	if len(packet) == 0 {
		return nil, fmt.Errorf("decode Opus packet (pion): empty packet (use Conceal for lost frames)")
	}

	decodedSamples, err := pd.decoder.DecodeToInt16(packet, pd.sampleBuf)
	if err != nil {
		return nil, fmt.Errorf("decode Opus packet (pion): %w", err)
	}

	// The caller is expected to copy this before the next Decode/Conceal
	// call, since the backing array is reused.
	for index := 0; index < decodedSamples*pd.channels; index++ {
		pd.outBuf[index*2] = byte(pd.sampleBuf[index])
		pd.outBuf[index*2+1] = byte(pd.sampleBuf[index] >> 8)
	}
	return pd.outBuf[:decodedSamples*pd.channels*2], nil
}

// Conceal always fails: pion/opus's current public Decoder rejects an
// empty/nil input as a malformed packet rather than treating it as a loss
// signal the way libopus does (see hraban/opus's Conceal). Reporting this
// explicitly means concealForGap in grpc_follower_stream.go will fall back
// to plain silence for gaps when using the pion decoder, rather than this
// silently producing wrong output. Worth revisiting if pion/opus adds PLC
// support later.
func (pd *PionOpusDecoder) Conceal() ([]byte, error) {
	return nil, fmt.Errorf("pion Opus decoder does not support concealment")
}

func (pd *PionOpusDecoder) Close() error {
	return nil
}

func (pd *PionOpusDecoder) Reset() error {
	decoder, err := pionopus.NewDecoderWithOutput(pd.sampleRate, pd.channels)
	if err != nil {
		return fmt.Errorf("reset pion Opus decoder: %w", err)
	}
	pd.decoder = decoder
	return nil
}
