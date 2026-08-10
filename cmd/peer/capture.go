package main

import (
	"fmt"
	"io"
	"log"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"

	"silent/internal/control"
)

const (
	streamSampleRate   = 48000
	streamChannels     = 2
	streamSampleFormat = "s16le"
	streamChunkBytes   = 3840
)

type streamFormat struct {
	SampleRate   int
	Channels     int
	SampleFormat string
	ChunkBytes   int
}

func (s *peerControlServer) openStreamSource(req *control.StreamPlaybackRequest) (io.ReadCloser, string, func() error, error) {
	if s.isLeader && s.liveCapture {
		rc, closeFn, err := startLiveCaptureSource(s.captureDevice)
		if err != nil {
			return nil, "", nil, fmt.Errorf("start live capture source: %w", err)
		}
		return rc, "live-capture", closeFn, nil
	}

	if req.AudioPath == "" {
		return nil, "", nil, fmt.Errorf("audio_path is required when live capture is disabled")
	}

	rc, closeFn, err := startFilePCMSource(req.AudioPath)
	if err != nil {
		return nil, "", nil, fmt.Errorf("start file audio source: %w", err)
	}

	return rc, req.AudioPath, closeFn, nil
}

func startLiveCaptureSource(device string) (io.ReadCloser, func() error, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return nil, nil, fmt.Errorf("ffmpeg is required for live capture")
	}

	dev := normalizeCaptureDevice(device)

	var args []string
	switch runtime.GOOS {
	case "linux":
		args = append(args,
			"-f", "pulse",
			"-i", dev,
		)
	case "windows":
		if dev == "default" {
			return nil, nil, fmt.Errorf("windows capture format dshow requires an explicit capture-device (run: ffmpeg -list_devices true -f dshow -i dummy)")
		}

		args = append(args,
			"-f", "dshow",
			"-i", "audio="+dev,
		)
	default:
		return nil, nil, fmt.Errorf("live capture is not supported on %s", runtime.GOOS)
	}

	args = appendPCMOutputArgs(args)

	return startFFmpegPipe(args, dev)
}

func startFilePCMSource(audioPath string) (io.ReadCloser, func() error, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return nil, nil, fmt.Errorf("ffmpeg is required for file audio transcoding")
	}

	cleanPath := filepath.Clean(audioPath)
	args := []string{
		"-hide_banner",
		"-loglevel", "error",
		"-i", cleanPath,
	}
	args = appendPCMOutputArgs(args)

	return startFFmpegPipe(args, cleanPath)
}

func appendPCMOutputArgs(args []string) []string {
	return append(args,
		"-vn",
		"-sn",
		"-dn",
		"-ac", fmt.Sprintf("%d", streamChannels),
		"-ar", fmt.Sprintf("%d", streamSampleRate),
		"-f", streamSampleFormat,
		"pipe:1",
	)
}

func startFFmpegPipe(args []string, sourceName string) (io.ReadCloser, func() error, error) {
	cmd := exec.Command("ffmpeg", args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logExecError("capture", cmd, "stdout-pipe", err)
		return nil, nil, fmt.Errorf("ffmpeg stdout pipe: %w", err)
	}

	log.Printf("capture: source=%q", sourceName)
	logExecStart("capture", cmd)
	if err := cmd.Start(); err != nil {
		logExecError("capture", cmd, "start", err)
		_ = stdout.Close()
		return nil, nil, fmt.Errorf("start ffmpeg source %q: %w", sourceName, err)
	}

	closeAndWait := func() error {
		_ = stdout.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		err := cmd.Wait()
		if err != nil {
			logExecError("capture", cmd, "wait", err)
		}
		return err
	}

	return stdout, closeAndWait, nil
}

func defaultStreamFormat() streamFormat {
	return streamFormat{
		SampleRate:   streamSampleRate,
		Channels:     streamChannels,
		SampleFormat: streamSampleFormat,
		ChunkBytes:   streamChunkBytes,
	}
}

func normalizeStreamPlaybackRequest(req *control.StreamPlaybackRequest) streamFormat {
	format := defaultStreamFormat()

	if req.GetSampleRate() > 0 {
		format.SampleRate = int(req.GetSampleRate())
	}
	if req.GetChannels() > 0 {
		format.Channels = int(req.GetChannels())
	}
	if strings.TrimSpace(req.GetSampleFormat()) != "" {
		format.SampleFormat = strings.TrimSpace(req.GetSampleFormat())
	}

	req.SampleRate = uint32(format.SampleRate)
	req.Channels = uint32(format.Channels)
	req.SampleFormat = format.SampleFormat

	return format
}

func normalizeCaptureDevice(device string) string {
	dev := strings.TrimSpace(device)
	if dev == "" {
		return "default"
	}
	if strings.EqualFold(dev, "default") {
		return "default"
	}
	return dev
}

func captureInputFormat() (string, error) {
	switch runtime.GOOS {
	case "linux":
		return "pulse", nil
	case "windows":
		return "dshow", nil
	default:
		return "", fmt.Errorf("live capture is not supported on %s", runtime.GOOS)
	}
}

func validateCaptureDevice(device string) error {
	dev := normalizeCaptureDevice(device)
	if dev == "default" {
		return nil
	}

	switch runtime.GOOS {
	case "linux":
		if _, err := exec.LookPath("pactl"); err != nil {
			return nil
		}

		out, err := exec.Command("pactl", "list", "short", "sources").CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to inspect linux sources via pactl: %w", err)
		}

		lines := strings.Split(string(out), "\n")
		known := make([]string, 0, len(lines))
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			name := strings.TrimSpace(fields[1])
			if name != "" {
				known = append(known, name)
			}
		}

		if slices.Contains(known, dev) {
			return nil
		}

		sort.Strings(known)
		if len(known) == 0 {
			return fmt.Errorf("capture-device %q not found and no PulseAudio/PipeWire sources were listed", dev)
		}
		return fmt.Errorf("capture-device %q not found; available sources: %s", dev, strings.Join(known, ", "))

	case "windows":
		if dev == "default" {
			return fmt.Errorf("capture-device %q is not valid for dshow; specify a named device via: ffmpeg -list_devices true -f dshow -i dummy", dev)
		}

		out, _ := exec.Command("ffmpeg", "-hide_banner", "-list_devices", "true", "-f", "dshow", "-i", "dummy").CombinedOutput()
		listing := strings.ToLower(string(out))
		if strings.Contains(listing, strings.ToLower(dev)) {
			return nil
		}
		return fmt.Errorf("capture-device %q not found in DirectShow list; run: ffmpeg -list_devices true -f dshow -i dummy", dev)

	default:
		return fmt.Errorf("capture device validation is not supported on %s", runtime.GOOS)
	}
}
