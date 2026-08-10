package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func schedulePlayback(at time.Time, wavPath string) {
	delay := time.Until(at)
	if delay > 0 {
		time.Sleep(delay)
	}

	if wavPath != "" {
		if _, err := exec.LookPath("ffplay"); err == nil {
			cmd := exec.Command("ffplay", "-nodisp", "-autoexit", "-loglevel", "error", wavPath)
			logExecStart("playback", cmd)
			if err := cmd.Start(); err != nil {
				logExecError("playback", cmd, "start", err)
				log.Printf("playback failed: %v", err)
			}
			return
		}

		if _, err := exec.LookPath("aplay"); err == nil {
			cmd := exec.Command("aplay", wavPath)
			logExecStart("playback", cmd)
			if err := cmd.Start(); err != nil {
				logExecError("playback", cmd, "start", err)
				log.Printf("playback failed: %v", err)
			}
			return
		}

		log.Printf("no audio player available for %s", wavPath)
		return
	}

	fmt.Print("\a")
}

func startStreamingPlayback() (io.WriteCloser, func() error, error) {
	stdin, closeFn, _, err := startStreamingPlaybackWithFormatAndLog(defaultStreamFormat(), "default")
	return stdin, closeFn, err
}

func scheduleRawPlayback(at time.Time, audioPath string, format streamFormat) {
	delay := time.Until(at)
	if delay > 0 {
		time.Sleep(delay)
	}

	if audioPath == "" {
		fmt.Print("\a")
		return
	}

	if _, err := exec.LookPath("ffplay"); err == nil {
		args := []string{"-nodisp", "-autoexit", "-loglevel", "error", "-f", format.SampleFormat, "-ar", fmt.Sprintf("%d", format.SampleRate)}
		args = append(args, ffplayChannelArgs(format.Channels)...)
		args = append(args, audioPath)
		cmd := exec.Command("ffplay", args...)
		logExecStart("raw-playback", cmd)
		if err := cmd.Start(); err != nil {
			logExecError("raw-playback", cmd, "start", err)
			log.Printf("raw playback failed: %v", err)
		}
		return
	}

	if _, err := exec.LookPath("aplay"); err == nil && format.SampleFormat == streamSampleFormat {
		cmd := exec.Command("aplay", "-t", "raw", "-f", "S16_LE", "-c", fmt.Sprintf("%d", format.Channels), "-r", fmt.Sprintf("%d", format.SampleRate), audioPath)
		logExecStart("raw-playback", cmd)
		if err := cmd.Start(); err != nil {
			logExecError("raw-playback", cmd, "start", err)
			log.Printf("raw playback failed: %v", err)
		}
		return
	}

	log.Printf("no raw audio player available for %s", audioPath)
}

func startStreamingPlaybackWithFormat(format streamFormat) (io.WriteCloser, func() error, error) {
	stdin, closeFn, _, err := startStreamingPlaybackWithFormatAndLog(format, "default")
	return stdin, closeFn, err
}

func startStreamingPlaybackWithFormatAndLog(format streamFormat, sessionID string) (io.WriteCloser, func() error, string, error) {
	if _, err := exec.LookPath("ffplay"); err != nil {
		return nil, nil, "", fmt.Errorf("ffplay is not available")
	}

	logPath := ffplayLogPath(sessionID)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, "", fmt.Errorf("create ffplay log file: %w", err)
	}

	args := []string{"-nodisp", "-autoexit", "-loglevel", "error", "-f", format.SampleFormat, "-ar", fmt.Sprintf("%d", format.SampleRate)}
	args = append(args, ffplayChannelArgs(format.Channels)...)
	args = append(args, "-i", "pipe:0")
	cmd := exec.Command("ffplay", args...)
	cmd.Stderr = newTimestampPrefixWriter(logFile)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		logExecError("stream-playback", cmd, "stdin-pipe", err)
		_ = logFile.Close()
		return nil, nil, "", fmt.Errorf("create ffplay stdin pipe: %w", err)
	}

	logExecStart("stream-playback", cmd)
	if err := cmd.Start(); err != nil {
		logExecError("stream-playback", cmd, "start", err)
		_ = stdin.Close()
		_ = logFile.Close()
		return nil, nil, "", fmt.Errorf("start ffplay: %w", err)
	}

	closeAndWait := func() error {
		_ = stdin.Close()
		err := cmd.Wait()
		if err != nil {
			logExecError("stream-playback", cmd, "wait", err)
		}
		_ = logFile.Close()
		return err
	}

	return stdin, closeAndWait, logPath, nil
}

func ffplayLogPath(sessionID string) string {
	name := sanitizeForFilename(sessionID)
	if strings.TrimSpace(name) == "" {
		name = "default"
	}
	return filepath.Join(os.TempDir(), fmt.Sprintf("silent-ffplay-%s.log", name))
}

func ffplayChannelArgs(channels int) []string {
	if channels <= 1 {
		return []string{"-ch_layout", "mono"}
	}
	if channels == 2 {
		return []string{"-ch_layout", "stereo"}
	}
	// Fallback for unusual channel counts.
	return []string{"-ch_layout", fmt.Sprintf("%dc", channels)}
}

type timestampPrefixWriter struct {
	file        *os.File
	atLineStart bool
}

func newTimestampPrefixWriter(file *os.File) io.Writer {
	return &timestampPrefixWriter{file: file, atLineStart: true}
}

func (w *timestampPrefixWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		if w.atLineStart {
			prefix := time.Now().Format(time.RFC3339Nano) + " "
			if _, err := w.file.WriteString(prefix); err != nil {
				return written, err
			}
			w.atLineStart = false
		}

		i := strings.IndexByte(string(p), '\n')
		if i == -1 {
			n, err := w.file.Write(p)
			written += n
			if err != nil {
				return written, err
			}
			return written, nil
		}

		chunk := p[:i+1]
		n, err := w.file.Write(chunk)
		written += n
		if err != nil {
			return written, err
		}
		w.atLineStart = true
		p = p[i+1:]
	}

	return written, nil
}
