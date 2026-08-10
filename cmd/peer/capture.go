package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"

	"silent/internal/control"
)

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

	f, err := os.Open(req.AudioPath)
	if err != nil {
		return nil, "", nil, fmt.Errorf("open audio file: %w", err)
	}

	return f, req.AudioPath, f.Close, nil
}

func startLiveCaptureSource(device string) (io.ReadCloser, func() error, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return nil, nil, fmt.Errorf("ffmpeg is required for live capture")
	}

	dev := normalizeCaptureDevice(device)

	var args []string
	switch runtime.GOOS {
	case "linux":
		args = []string{
			"-hide_banner",
			"-loglevel", "error",
			"-f", "pulse",
			"-i", dev,
			"-ac", "2",
			"-ar", "48000",
			"-f", "wav",
			"pipe:1",
		}
	case "windows":
		if dev == "default" {
			return nil, nil, fmt.Errorf("windows capture format dshow requires an explicit capture-device (run: ffmpeg -list_devices true -f dshow -i dummy)")
		}

		args = []string{
			"-hide_banner",
			"-loglevel", "error",
			"-f", "dshow",
			"-i", "audio=" + dev,
			"-ac", "2",
			"-ar", "48000",
			"-f", "wav",
			"pipe:1",
		}
	default:
		return nil, nil, fmt.Errorf("live capture is not supported on %s", runtime.GOOS)
	}

	cmd := exec.Command("ffmpeg", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("ffmpeg stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		return nil, nil, fmt.Errorf("start ffmpeg capture for device %q: %w", dev, err)
	}

	closeAndWait := func() error {
		_ = stdout.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return cmd.Wait()
	}

	return stdout, closeAndWait, nil
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

		for _, name := range known {
			if name == dev {
				return nil
			}
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
