package main

import (
	"fmt"
	"log"
	"os/exec"
)

func validateMediaRuntime(cfg config) error {
	if cfg.leader && cfg.liveCapture {
		if _, err := exec.LookPath("ffmpeg"); err != nil {
			return fmt.Errorf("leader live capture requires ffmpeg in PATH")
		}

		format, err := captureInputFormat()
		if err != nil {
			return err
		}

		if err := validateCaptureDevice(cfg.captureDevice); err != nil {
			return err
		}

		log.Printf("media check: live capture ready (input=%s device=%q)", format, normalizeCaptureDevice(cfg.captureDevice))
	}

	if _, err := exec.LookPath("ffplay"); err != nil {
		log.Printf("media check: ffplay not found; streamed playback may fail: %v", err)
	} else {
		log.Printf("media check: ffplay found")
	}

	return nil
}
