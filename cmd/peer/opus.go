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
)

type audioEncoder interface {
	EncodePCM(pcm []byte) ([]byte, error)
	Close() error
}

type audioDecoder interface {
	DecodePacket(packet []byte) ([]byte, error)
	Close() error
}

type OpusEncoder struct {
	encoder       *opus.Encoder
	sampleRate    int
	channels      int
	frameSamples  int // per channel: 960 for 20 ms at 48 kHz
	framePCMBytes int // 3840 for stereo s16le
}

type OpusDecoder struct {
	decoder       *opus.Decoder
	sampleRate    int
	channels      int
	frameSamples  int // per channel: 960 for 20 ms at 48 kHz
	framePCMBytes int // 3840 for stereo s16le
}

func newOpusEncoder(sampleRate int, channels int) (*OpusEncoder, error) {
	encoder, err := opus.NewEncoder(sampleRate, channels, opus.Application(opus.AppAudio))
	if err != nil {
		return nil, err
	}
	if err := encoder.SetBitrate(opus320kbpsBitrate); err != nil {
		return nil, fmt.Errorf("set Opus bitrate: %w", err)
	}
	frameSamples := sampleRate * int(opusFrameDuration) / int(time.Second)
	framePCMBytes := frameSamples * channels * 2
	return &OpusEncoder{encoder: encoder, sampleRate: sampleRate, channels: channels, frameSamples: frameSamples, framePCMBytes: framePCMBytes}, nil
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
	samples := make([]int16, oe.frameSamples*oe.channels)
	for index := range samples {
		samples[index] = int16(pcm[index*2]) | int16(pcm[index*2+1])<<8
	}

	// Encode the samples to Opus
	opusData := make([]byte, 4000) // Allocate a buffer for the encoded data
	encodedBytes, err := oe.encoder.Encode(samples, opusData)
	if err != nil {
		return nil, fmt.Errorf("encode Opus frame: %w", err)
	}

	return opusData[:encodedBytes], nil
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
	return &OpusDecoder{decoder: decoder, sampleRate: sampleRate, channels: channels, frameSamples: frameSamples, framePCMBytes: framePCMBytes}, nil
}

func (od *OpusDecoder) DecodePacket(packet []byte) ([]byte, error) {
	// Decode the Opus packet to PCM samples
	samples := make([]int16, od.frameSamples*od.channels) // Allocate a buffer for the decoded samples
	decodedSamples, err := od.decoder.Decode(packet, samples)
	if err != nil {
		return nil, fmt.Errorf("decode Opus packet: %w", err)
	}

	// Convert int16 samples to PCM bytes
	pcm := make([]byte, decodedSamples*od.channels*2)
	for index := 0; index < decodedSamples*od.channels; index++ {
		pcm[index*2] = byte(samples[index])
		pcm[index*2+1] = byte(samples[index] >> 8)
	}

	return pcm, nil
}

func (od *OpusDecoder) Close() error {
	// No resources to release for the Opus decoder in this implementation
	return nil
}
