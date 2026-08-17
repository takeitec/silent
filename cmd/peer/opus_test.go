package main

import (
	"testing"
)

func TestOpusEncoderInitialization(t *testing.T) {
	encoder, err := newOpusEncoder(48000, 2, opus128kbpsBitrate)
	if err != nil {
		t.Fatalf("Failed to initialize Opus encoder: %v", err)
	}
	defer encoder.Close()

	if encoder.sampleRate != 48000 {
		t.Errorf("Expected sample rate 48000, got %d", encoder.sampleRate)
	}
	if encoder.channels != 2 {
		t.Errorf("Expected channels 2, got %d", encoder.channels)
	}
	if encoder.frameSamples != 960 {
		t.Errorf("Expected frame samples 960, got %d", encoder.frameSamples)
	}
	if encoder.framePCMBytes != 3840 {
		t.Errorf("Expected frame PCM bytes 3840, got %d", encoder.framePCMBytes)
	}
}

func TestOpusEncoderRejectsPartialFrame(t *testing.T) {
	encoder, err := newOpusEncoder(48000, 2, opus128kbpsBitrate)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()

	_, err = encoder.EncodePCM(make([]byte, 3838))
	if err == nil {
		t.Fatal("expected partial frame to be rejected")
	}
}

func TestOpusEncoderEncodesOneFrame(t *testing.T) {
	encoder, err := newOpusEncoder(48000, 2, opus128kbpsBitrate)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()

	packet, err := encoder.EncodePCM(make([]byte, 3840))
	if err != nil {
		t.Fatal(err)
	}
	if len(packet) == 0 {
		t.Fatal("expected non-empty Opus packet")
	}
}

func TestOpusDecoderInitialization(t *testing.T) {
	decoder, err := newOpusDecoder(48000, 2)
	if err != nil {
		t.Fatalf("Failed to initialize Opus decoder: %v", err)
	}
	defer decoder.Close()

	if decoder.sampleRate != 48000 {
		t.Errorf("Expected sample rate 48000, got %d", decoder.sampleRate)
	}
	if decoder.channels != 2 {
		t.Errorf("Expected channels 2, got %d", decoder.channels)
	}
	if decoder.frameSamples != 960 {
		t.Errorf("Expected frame samples 960, got %d", decoder.frameSamples)
	}
	if decoder.framePCMBytes != 3840 {
		t.Errorf("Expected frame PCM bytes 3840, got %d", decoder.framePCMBytes)
	}
}

func TestOpusDecoderDecodesOneFrame(t *testing.T) {
	encoder, err := newOpusEncoder(48000, 2, opus128kbpsBitrate)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()

	decoder, err := newOpusDecoder(48000, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	packet, err := encoder.EncodePCM(make([]byte, 3840))
	if err != nil {
		t.Fatal(err)
	}

	pcm, err := decoder.DecodePacket(packet)
	if err != nil {
		t.Fatal(err)
	}
	if len(pcm) != 3840 {
		t.Fatalf("expected decoded PCM length 3840, got %d", len(pcm))
	}
}

func TestOpusDecoderRejectsInvalidPacket(t *testing.T) {
	decoder, err := newOpusDecoder(48000, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	_, err = decoder.DecodePacket(nil)
	if err == nil {
		t.Fatal("expected empty packet to be rejected; use Conceal for lost frames")
	}
}

func TestOpusEncoderAndDecoderIntegration(t *testing.T) {
	encoder, err := newOpusEncoder(48000, 2, opus128kbpsBitrate)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()

	decoder, err := newOpusDecoder(48000, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	originalPCM := make([]byte, 3840)
	for i := range originalPCM {
		originalPCM[i] = byte(i % 256)
	}

	packet, err := encoder.EncodePCM(originalPCM)
	if err != nil {
		t.Fatal(err)
	}

	decodedPCM, err := decoder.DecodePacket(packet)
	if err != nil {
		t.Fatal(err)
	}

	if len(packet) == 0 {
		t.Fatal("expected non-empty Opus packet")
	}
	if len(decodedPCM) != 3840 {
		t.Fatalf("expected 3840 decoded PCM bytes, got %d", len(decodedPCM))
	}
	if len(packet) >= len(originalPCM) {
		t.Fatalf("expected compressed packet smaller than PCM: packet=%d pcm=%d",
			len(packet), len(originalPCM))
	}
}

func TestOpusDecoderConcealsLostFrame(t *testing.T) {
	encoder, err := newOpusEncoder(48000, 2, opus128kbpsBitrate)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()

	decoder, err := newOpusDecoder(48000, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()

	// Decode one real frame so the decoder has state to conceal from.
	packet, err := encoder.EncodePCM(make([]byte, 3840))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decoder.DecodePacket(packet); err != nil {
		t.Fatalf("decode real frame: %v", err)
	}

	// Simulate the next frame never arriving.
	concealed, err := decoder.Conceal()
	if err != nil {
		t.Fatalf("Conceal: %v", err)
	}
	if len(concealed) != 3840 {
		t.Fatalf("expected concealed frame length 3840, got %d", len(concealed))
	}

	// Decoder should recover cleanly and keep decoding after concealment.
	packet2, err := encoder.EncodePCM(make([]byte, 3840))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decoder.DecodePacket(packet2); err != nil {
		t.Fatalf("decode after concealment: %v", err)
	}
}
