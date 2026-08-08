package main

import (
	"fmt"
	"log"
	"os/exec"
	"runtime"
	"time"
)

func schedulePlayback(at time.Time, wavPath string) {
	delay := time.Until(at)
	if delay > 0 {
		time.Sleep(delay)
	}

	if wavPath != "" {
		if runtime.GOOS == "windows" {
			log.Printf("playback: Windows host detected; skipping external audio player")
			fmt.Print("\a")
			return
		}

		if _, err := exec.LookPath("aplay"); err == nil {
			cmd := exec.Command("aplay", wavPath)
			if err := cmd.Start(); err != nil {
				log.Printf("playback failed: %v", err)
			}
			return
		}

		if _, err := exec.LookPath("ffplay"); err == nil {
			cmd := exec.Command("ffplay", "-nodisp", "-autoexit", wavPath)
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
