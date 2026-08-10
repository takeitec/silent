package main

import (
	"fmt"
	"io"
	"log"
	"os/exec"
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
			if err := cmd.Start(); err != nil {
				log.Printf("playback failed: %v", err)
			}
			return
		}

		if _, err := exec.LookPath("aplay"); err == nil {
			cmd := exec.Command("aplay", wavPath)
			if err := cmd.Start(); err != nil {
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
	if _, err := exec.LookPath("ffplay"); err != nil {
		return nil, nil, fmt.Errorf("ffplay is not available")
	}

	cmd := exec.Command("ffplay", "-nodisp", "-autoexit", "-loglevel", "error", "-i", "pipe:0")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("create ffplay stdin pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return nil, nil, fmt.Errorf("start ffplay: %w", err)
	}

	closeAndWait := func() error {
		_ = stdin.Close()
		return cmd.Wait()
	}

	return stdin, closeAndWait, nil
}
